package org.hibernate.search.test;


public class SearchAsyncTester {
        private Thread thread;
        private volatile Error error;
        private volatile RuntimeException runtimeExc;

        public SearchAsyncTester(final Runnable runnable) {
            thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        runnable.run();
                    } catch (Error e) {
                        error = e;
                    } catch (RuntimeException e) {
                        runtimeExc = e;
                    }
                }
            });
            
        }

        public void test() throws InterruptedException {
            thread.join();
            if (error != null) {
                throw error;
            }
            if (runtimeExc != null) {
                throw runtimeExc;
            }
        }
        
        public void start() {
            thread.start();
        }
}
