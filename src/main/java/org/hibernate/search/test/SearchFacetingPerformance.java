package org.hibernate.search.test;

import java.util.List;

import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.util.Version;
import org.openjdk.jmh.annotations.BenchmarkMode;
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

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.hibernate.search.FullTextQuery;
import org.hibernate.search.FullTextSession;
import org.hibernate.search.Search;
import org.hibernate.search.query.dsl.QueryBuilder;
import org.hibernate.search.query.facet.Facet;
import org.hibernate.search.query.facet.FacetSortOrder;
import org.hibernate.search.query.facet.FacetingRequest;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(MICROSECONDS)
@Warmup(iterations = 2, time = 1, timeUnit = SECONDS)
@Measurement(iterations = 2, time = 1, timeUnit = SECONDS)
//@Fork(5)
@State(Scope.Benchmark)
@Threads(1)
public class SearchFacetingPerformance {
	private SessionFactory sessionFactory;
	private Author voltaire;
	private Author hugo;
	private Author moliere;
	private Author proust;

	@Setup
	public void setUp() {
		Configuration configuration = buildConfiguration();
		sessionFactory = configuration.buildSessionFactory();
		createTestData();
	}

	@TearDown
	public void tearDown() {
		sessionFactory.close();
	}

	@GenerateMicroBenchmark
	public void hsearchFaceting() {
		FullTextSession fullTextSession = Search.getFullTextSession( sessionFactory.openSession() );

		FullTextQuery fullTextQuery = fullTextSession.createFullTextQuery( new MatchAllDocsQuery(), Book.class );

		QueryBuilder builder = fullTextSession.getSearchFactory().buildQueryBuilder().forEntity( Book.class ).get();
		FacetingRequest facetReq = builder.facet()
				.name( "someFacet" )
				.onField( "authors.name_untokenized" )
				.discrete()
				.orderedBy( FacetSortOrder.COUNT_DESC )
				.includeZeroCounts( false ).maxFacetCount( 10 )
				.createFacetingRequest();

		List<Facet> facets = fullTextQuery.getFacetManager().enableFaceting( facetReq ).getFacets( "someFacet" );
		assert ( facets.size() == 3 );

		fullTextSession.close();
	}

	@GenerateMicroBenchmark
	public void luceneFaceting() {
		hsearchFaceting();
	}

	public static void main(String args[]) {
		SearchFacetingPerformance performance = new SearchFacetingPerformance();
		performance.setUp();
		performance.hsearchFaceting();
	}

	private Configuration buildConfiguration() {
		Configuration cfg = new Configuration();

		// ORM config
		cfg.setProperty( Environment.DIALECT, "org.hibernate.dialect.H2Dialect" );
		cfg.setProperty( Environment.DRIVER, "org.h2.Driver" );
		cfg.setProperty( Environment.URL, "jdbc:h2:mem:db1;DB_CLOSE_DELAY=-1" );
		cfg.setProperty( Environment.USER, "sa" );
		cfg.setProperty( Environment.PASS, "" );
		cfg.setProperty( Environment.HBM2DDL_AUTO, "create-drop" );
		cfg.setProperty( Environment.SHOW_SQL, "false" );
		cfg.setProperty( Environment.FORMAT_SQL, "false" );

		// Search config
		cfg.setProperty( "hibernate.search.lucene_version", Version.LUCENE_CURRENT.name() );
		cfg.setProperty( "hibernate.search.default.directory_provider", "ram" );
		cfg.setProperty( org.hibernate.search.Environment.ANALYZER_CLASS, StopAnalyzer.class.getName() );
		cfg.setProperty( "hibernate.search.default.indexwriter.merge_factor", "100" );
		cfg.setProperty( "hibernate.search.default.indexwriter.max_buffered_docs", "1000" );

		// configured classes
		cfg.addAnnotatedClass( Book.class );
		cfg.addAnnotatedClass( Author.class );

		return cfg;
	}


	public void createTestData() {
		voltaire = new Author();
		voltaire.setName( "Voltaire" );

		hugo = new Author();
		hugo.setName( "Victor Hugo" );

		moliere = new Author();
		moliere.setName( "Moliere" );

		proust = new Author();
		proust.setName( "Proust" );

		Book book1 = new Book();
		book1.setName( "Candide" );
		book1.getAuthors().add( hugo );
		book1.getAuthors().add( voltaire );

		Book book2 = new Book();
		book2.setName( "Amphitryon" );
		book2.getAuthors().add( hugo );
		book2.getAuthors().add( moliere );

		Book book3 = new Book();
		book3.setName( "Hernani" );
		book3.getAuthors().add( hugo );
		book3.getAuthors().add( moliere );

		Session session = sessionFactory.openSession();
		Transaction tx = session.beginTransaction();
		session.persist( voltaire );
		session.persist( hugo );
		session.persist( moliere );
		session.persist( proust );
		session.persist( book1 );
		session.persist( book2 );
		session.persist( book3 );

		tx.commit();
		session.close();
	}
}
