package org.hibernate.search.test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.facet.params.CategoryListParams;
import org.apache.lucene.facet.params.FacetIndexingParams;
import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.search.CountFacetRequest;
import org.apache.lucene.facet.search.FacetRequest;
import org.apache.lucene.facet.search.FacetResult;
import org.apache.lucene.facet.search.FacetsCollector;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesAccumulator;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetFields;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryParser.MultiFieldQueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.hibernate.CacheMode;
import org.hibernate.FlushMode;
import org.hibernate.ScrollMode;
import org.hibernate.ScrollableResults;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.hibernate.search.FullTextQuery;
import org.hibernate.search.FullTextSession;
import org.hibernate.search.Search;
import org.hibernate.search.query.dsl.QueryBuilder;
import org.hibernate.search.query.engine.spi.FacetManager;
import org.hibernate.search.query.facet.Facet;
import org.hibernate.search.query.facet.FacetSortOrder;
import org.hibernate.search.query.facet.FacetingRequest;
import org.hibernate.search.test.Author;
import org.hibernate.search.test.Book;
import org.hibernate.search.test.SearchAsyncTester;
import org.hibernate.search.test.TestObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.GenerateMicroBenchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(MILLISECONDS)
@Warmup(iterations = 1, time = 1, timeUnit = SECONDS)
@Measurement(iterations = 2, time = 1, timeUnit = SECONDS)
@State(Scope.Benchmark)
@Fork(5)
@Threads(1)
public class SearchFacetingPerformance {
    private static final int BATCH_SIZE = 250;
    private static final String HSEARCH_LUCENE_INDEX_DIR = "hsearch-lucene";
    private static final String NATIVE_LUCENE_INDEX_DIR = "native-lucene";

    private static final String AUTHOR_NAME_FACET = "authorNameFacet";
    private static final String PUBLISHER_NAME_FACET = "publisherNameFacet";
    private static final String ISBN_NAME_FACET = "isbnFacet";
    private SessionFactory sessionFactory;

     private IndexSearcher searcher;
     private FullTextSession fullTextSession;

     @Setup
    @Before
    public void setUp() throws Exception {
        Configuration configuration = buildConfiguration();
        sessionFactory = configuration.buildSessionFactory();
        System.out.println("begin indexing");
        if (needsIndexing()) {
            createNativeLuceneIndex();
            indexTestData();
        }
        searcher = getIndexSearcher();
        System.out.println("complete indexing");
    }

    @TearDown
    @After
    public void tearDown() {
        sessionFactory.close();
    }

    @GenerateMicroBenchmark
    public void hsearchFaceting() {
        FullTextSession fullTextSession = Search.getFullTextSession(sessionFactory.openSession());

        FullTextQuery fullTextQuery = fullTextSession.createFullTextQuery(new MatchAllDocsQuery(), Book.class);

        QueryBuilder builder = fullTextSession.getSearchFactory().buildQueryBuilder().forEntity(Book.class).get();
        FacetingRequest facetReq = builder.facet()
                .name(AUTHOR_NAME_FACET)
                .onField("authors.name_untokenized")
                .discrete()
                .orderedBy(FacetSortOrder.COUNT_DESC)
                .includeZeroCounts(false)
                .maxFacetCount(10)
                .createFacetingRequest();

        List<Facet> facets = fullTextQuery.getFacetManager().enableFaceting(facetReq).getFacets(AUTHOR_NAME_FACET);
        assertEquals("Wrong facet count", 10, facets.size());
        assertEquals("Wrong facet ", "Bittinger, Marvin L.", facets.get(0).getValue());
        assertEquals("Wrong facet value count", 169, facets.get(0).getCount());

        fullTextSession.close();
    }

    @Test
    public void hsearchFacetingTest() throws Exception, InterruptedException {

        // setup a bunch of queries, assert a couple of facet quantities
        List<TestObject> queries = new ArrayList<TestObject>();
        queries.add(new TestObject("title:{A TO L}", 1540, 930, 26960, 7179));
        queries.add(new TestObject("*:* or *:* or *:* or *:* and *:* or isbn:1*", -1, -1, -1, -1));
        queries.add(new TestObject("title:visual or title:Visual", -1, -1, -1, -1));
        queries.add(new TestObject("title:{L TO Z}", -1, -1, -1, -1));
        queries.add(new TestObject("title:{A TO L}", 1540, 930, 26960, 7179));
        queries.add(new TestObject("isbn:{1 TO 5}", -1, -1, -1, -1));
        queries.add(new TestObject("book.authors:{A TO B}", -1, -1, -1, -1));
        queries.add(new TestObject("title:{A TO L}", 1540, 930, 26960, 7179));

        // setup AsyncTester array
        final SearchAsyncTester[] testers = new SearchAsyncTester[queries.size()];
        final SearchFacetingPerformance perfTester = this;
        for (int i = 0; i < queries.size(); i++) {
            final TestObject query = queries.get(i);

            testers[i] = new SearchAsyncTester(new Runnable() {

                @Override
                public void run() {

                    System.out.println(String.format("\n%s: Start: %s\n", query.getQuery(), System.currentTimeMillis()));
                    try {

                        // run query
                        Object[] result = perfTester.buildQuery(query.getQuery());
                        System.out.println(String.format("\n%s: End: %s\n", query.getQuery(), System.currentTimeMillis()));

                        // get the results  and the facets
                        FullTextQuery fullTextQuery = (FullTextQuery) result[0];
                        List<Facet> authorFacets = (List<Facet>) result[1];
                        List<Facet> publisherFacets = (List<Facet>) result[2];
                        System.out.println(String.format("%s: Total Results: %s", query, fullTextQuery.getResultSize()));
                        System.out.println(String.format("\t%s: %s -- %s", query.getQuery(), authorFacets.size(), authorFacets));
                        System.out.println(String.format("\t%s: %s -- %s", query.getQuery(), publisherFacets.size(), publisherFacets));
                        // make assertions if values are != -1
                        if (query.getAuthorFacetCount() > -1) {
                            assertEquals(String.format("wrong a1 facet count %s != %s", query.getAuthorFacetCount(), authorFacets.get(0).getCount()),
                                    query.getAuthorFacetCount(), authorFacets.get(0).getCount());
                        }
                        if (query.getAuthorFacetCount2() > -1) {
                            assertEquals(String.format("wrong a2 facet count %s != %s", query.getAuthorFacetCount2(), authorFacets.get(1).getCount()),
                                    query.getAuthorFacetCount2(), authorFacets.get(1).getCount());
                        }
                        if (query.getPublisherFacetCount() > -1) {
                            assertEquals(String.format("wrong p1 facet count %s != %s", query.getPublisherFacetCount(), publisherFacets.get(0).getCount()),
                                    query.getPublisherFacetCount(), publisherFacets.get(0).getCount());
                        }
                        if (query.getPublisherFacetCount2() > -1) {
                            assertEquals(String.format("wrong p2 facet count %s != %s", query.getPublisherFacetCount2(), publisherFacets.get(1).getCount()),
                                    query.getPublisherFacetCount2(), publisherFacets.get(1).getCount());
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }
            });
            // setup the thread
            testers[i].start();
            ;
        }

        for (SearchAsyncTester tester : testers) {
            // run the test -- should be effectively simultaneous
            if (tester != null) {
                tester.test();
            }
        }

    }

    public Object[] buildQuery(String query) throws Exception {
        FullTextSession fullTextSession = Search.getFullTextSession(sessionFactory.openSession());
        Transaction beginTransaction = fullTextSession.beginTransaction();
        // beginTransaction.begin();

        PerFieldAnalyzerWrapper analyzer = new PerFieldAnalyzerWrapper(new StandardAnalyzer(
                Version.LUCENE_46));

        MultiFieldQueryParser parser = new MultiFieldQueryParser(Version.LUCENE_46, new String[] { "title", "author.name" }, analyzer);
        FullTextQuery fullTextQuery = fullTextSession.createFullTextQuery(parser.parse(query), Book.class);

        fullTextQuery.setSort(new Sort(new SortField("publisher", SortField.Type.STRING, true), new SortField("title", SortField.Type.STRING), new SortField("isbn",
                SortField.Type.STRING, true)));
        fullTextQuery.setFirstResult(0);
        fullTextQuery.setMaxResults(1000);

        QueryBuilder builder = fullTextSession.getSearchFactory().buildQueryBuilder().forEntity(Book.class).get();
        FacetingRequest facetReq = builder.facet().name(AUTHOR_NAME_FACET).onField("authors.name_untokenized").discrete().orderedBy(FacetSortOrder.COUNT_DESC)
                .includeZeroCounts(false).createFacetingRequest();
        FacetingRequest facetReq3 = builder.facet().name(ISBN_NAME_FACET).onField("isbn").discrete().orderedBy(FacetSortOrder.COUNT_DESC)
                .includeZeroCounts(false).createFacetingRequest();
        QueryBuilder builder2 = fullTextSession.getSearchFactory().buildQueryBuilder().forEntity(Book.class).get();
        FacetingRequest facetReq2 = builder2.facet().name(PUBLISHER_NAME_FACET).onField("publisher").discrete().orderedBy(FacetSortOrder.COUNT_DESC)
                .includeZeroCounts(false).createFacetingRequest();
        FacetManager facetManager = fullTextQuery.getFacetManager();
        facetManager.enableFaceting(facetReq);
        facetManager.enableFaceting(facetReq2);
        facetManager.enableFaceting(facetReq3);
        List<Facet> facets = facetManager.getFacets(AUTHOR_NAME_FACET);
        List<Facet> facets2 = facetManager.getFacets(PUBLISHER_NAME_FACET);
        List<Facet> facets3 = facetManager.getFacets(ISBN_NAME_FACET);
        List list = fullTextQuery.list();
//        beginTransaction.commit();
//        fullTextSession.close();
        return new Object[] { fullTextQuery, facets, facets2 };
    }

    @GenerateMicroBenchmark
    public void luceneFaceting() throws Exception {
        // setup the facets
        List<FacetRequest> facetRequests = new ArrayList<FacetRequest>();
        facetRequests.add( new CountFacetRequest( new CategoryPath( "authors.name_untokenized" ), 1 ) );
        FacetSearchParams facetSearchParams = new FacetSearchParams( new FacetIndexingParams(), facetRequests );
        SortedSetDocValuesReaderState state = new SortedSetDocValuesReaderState(
                new FacetIndexingParams( new CategoryListParams( "authors.name_untokenized" ) ),
                searcher.getIndexReader()
        );
        FacetsCollector facetsCollector = FacetsCollector.create(
                new SortedSetDocValuesAccumulator(
                        state,
                        facetSearchParams
                )
        );

        // search
        searcher.search( new MatchAllDocsQuery(), facetsCollector );

        // get the facet result
        List<FacetResult> facets = facetsCollector.getFacetResults();
        assertEquals( "Wrong facet count", 1, facets.size() );
        FacetResult topFacetResult = facets.get( 0 );

        assertEquals(
                "Wrong facet ",
                "Bittinger, Marvin L.",
                topFacetResult.getFacetResultNode().subResults.get( 0 ).label.components[1]
        );
        assertEquals(
                "Wrong facet value count",
                169,
                (int) topFacetResult.getFacetResultNode().subResults.get( 0 ).value
        );
    }

    public static void main(String args[]) {
        SearchFacetingPerformance performance = new SearchFacetingPerformance();
        try {
            performance.setUp();
            performance.hsearchFaceting();
            performance.luceneFaceting();
            performance.tearDown();
        }
        catch ( Exception e ) {
            System.err.println( e.getMessage() );
            e.printStackTrace();
        }
    }


    private Configuration buildConfiguration() {
        Configuration cfg = new Configuration();

        // ORM config
        // cfg.setProperty( Environment.DIALECT, "org.hibernate.dialect.H2Dialect" );
        // cfg.setProperty( Environment.DRIVER, "org.h2.Driver" );
        // cfg.setProperty( Environment.URL, "jdbc:h2:mem:db1;DB_CLOSE_DELAY=-1" );
        // cfg.setProperty( Environment.USER, "sa" );
        // cfg.setProperty( Environment.PASS, "" );

        // cfg.setProperty( Environment.DIALECT, "org.hibernate.dialect.MySQL5InnoDBDialect" );
        // cfg.setProperty( Environment.DRIVER, "com.mysql.jdbc.Driver" );
        // cfg.setProperty( Environment.URL, "jdbc:mysql://localhost/hibernate" );
        // cfg.setProperty( Environment.USER, "hibernate" );
        // cfg.setProperty( Environment.PASS, "hibernate" );

        cfg.setProperty(Environment.DIALECT, "org.hibernate.dialect.PostgreSQLDialect");
        cfg.setProperty(Environment.DRIVER, "org.postgresql.Driver");
        cfg.setProperty(Environment.URL, "jdbc:postgresql://localhost/hibernate");
        cfg.setProperty(Environment.USER, "hibernate");
        cfg.setProperty(Environment.PASS, "hibernate");
        cfg.setProperty("hibernate.current_session_context_class", "thread");
        // cfg.setProperty( Environment.HBM2DDL_AUTO, "create" );
        cfg.setProperty(Environment.SHOW_SQL, "false");
        cfg.setProperty(Environment.FORMAT_SQL, "false");

        // Search config
        cfg.setProperty( "hibernate.search.lucene_version", Version.LUCENE_46.name() );
        cfg.setProperty( "hibernate.search.default.directory_provider", "filesystem" );
        cfg.setProperty( "hibernate.search.default.indexBase", HSEARCH_LUCENE_INDEX_DIR );
        cfg.setProperty( org.hibernate.search.Environment.ANALYZER_CLASS, StopAnalyzer.class.getName() );
        cfg.setProperty( "hibernate.search.default.indexwriter.merge_factor", "100" );
        cfg.setProperty( "hibernate.search.default.indexwriter.max_buffered_docs", "1000" );

        // configured classes
        cfg.addAnnotatedClass(Book.class);
        cfg.addAnnotatedClass(Author.class);

        return cfg;
    }

    private void indexTestData() throws Exception {

        FullTextSession fullTextSession = Search.getFullTextSession(sessionFactory.openSession());
        fullTextSession.setFlushMode(FlushMode.MANUAL);
        fullTextSession.setCacheMode(CacheMode.IGNORE);
        Transaction transaction = fullTextSession.beginTransaction();
        ScrollableResults results = fullTextSession.createCriteria(Book.class)
                .setFetchSize(BATCH_SIZE)
                .scroll(ScrollMode.FORWARD_ONLY);
        int index = 0;
        while (results.next()) {
            index++;
            Book book = (Book) results.get(0);
            System.out.println(book.getTitle());
            // let's make this a bit harder by putting lots more things in the lucene index
            indexBookHSearch(fullTextSession, book);
            add(fullTextSession, book, 1);
            add(fullTextSession, book, 2);
            add(fullTextSession, book, 3);
            add(fullTextSession, book, 4);
            add(fullTextSession, book, 5);
            add(fullTextSession, book, 6);
            add(fullTextSession, book, 7);
            add(fullTextSession, book, 8);
            add(fullTextSession, book, 9);
            // indexBookLucene(indexWriter, book);
            if (index % BATCH_SIZE == 0) {
                fullTextSession.flushToIndexes();
                fullTextSession.clear();
            }
        }
        fullTextSession.flushToIndexes();
        fullTextSession.clear();
        transaction.commit();
        fullTextSession.close();
    }

    private void add(FullTextSession fullTextSession, Book book, int i) {
        book.setTitle(book.getTitle() + i);
        book.setId(book.getId() + i * 10000);
        indexBookHSearch(fullTextSession, book);

    }

    private void indexBookHSearch(FullTextSession fullTextSession, Book book) {
        fullTextSession.index(book);
        fullTextSession.flushToIndexes();
    }

    private void indexBookLucene(IndexWriter writer, Book book) throws Exception {
        Document document = new Document();
        document.add( new TextField( "title", book.getTitle(), Field.Store.NO ) );
        document.add( new StringField( "isbn", book.getIsbn(), Field.Store.NO ) );
        document.add( new StringField( "publisher", book.getPublisher(), Field.Store.NO ) );
        SortedSetDocValuesFacetFields dvFields = new SortedSetDocValuesFacetFields(
                new FacetIndexingParams(
                        new CategoryListParams(
                                "authors.name_untokenized"
                        )
                )
        );
        List<CategoryPath> paths = new ArrayList<CategoryPath>();
        for ( Author author : book.getAuthors() ) {
            String name = author.getName();
            document.add( new TextField( "authors.name", name, Field.Store.NO ) );
            paths.add( new CategoryPath( "authors.name_untokenized", name ) );
        }
        if ( paths.size() > 0 ) {
            dvFields.addFields( document, paths );
        }
        writer.addDocument( document );
        writer.commit();
    }

    private boolean needsIndexing() {
        File nativeLuceneIndexDir = new File( NATIVE_LUCENE_INDEX_DIR );
        if ( !nativeLuceneIndexDir.exists() ) {
            return true;
        }

        File hsearchLuceneIndexDir = new File( HSEARCH_LUCENE_INDEX_DIR );
        if ( !hsearchLuceneIndexDir.exists() ) {
            return true;
        }

        return false;
    }

    private void createNativeLuceneIndex() throws Exception {
        boolean create = true;
        File indexDirFile = new File( NATIVE_LUCENE_INDEX_DIR );
        if ( indexDirFile.exists() && indexDirFile.isDirectory() ) {
            create = false;
        }

        Directory dir = FSDirectory.open( indexDirFile );
        Analyzer analyzer = new StandardAnalyzer( Version.LUCENE_46 );
        IndexWriterConfig iwc = new IndexWriterConfig( Version.LUCENE_46, analyzer );

        if ( create ) {
            // Create a new index in the directory, removing any
            // previously indexed documents:
            iwc.setOpenMode( IndexWriterConfig.OpenMode.CREATE );
        }

        IndexWriter writer = new IndexWriter( dir, iwc );
        writer.commit();
        writer.close( true );
    }

    private IndexWriter getIndexWriter() throws Exception {
        File indexDirFile = new File( NATIVE_LUCENE_INDEX_DIR );
        Directory dir = FSDirectory.open( indexDirFile );
        Analyzer analyzer = new StandardAnalyzer( Version.LUCENE_46 );
        IndexWriterConfig iwc = new IndexWriterConfig( Version.LUCENE_46, analyzer );
        iwc.setOpenMode( IndexWriterConfig.OpenMode.CREATE_OR_APPEND );
        return new IndexWriter( dir, iwc );
    }

    private IndexSearcher getIndexSearcher() {
        IndexReader indexReader;
        IndexSearcher indexSearcher = null;
        try {
            File indexDirFile = new File( NATIVE_LUCENE_INDEX_DIR );
            Directory dir = FSDirectory.open( indexDirFile );
            indexReader = DirectoryReader.open( dir );
            indexSearcher = new IndexSearcher( indexReader );
        }
        catch ( IOException ioe ) {
            ioe.printStackTrace();
        }

        return indexSearcher;
    }
}
