package com.willmadison.legacycodekatas.examples.wrapping;

public class Decorated {

    private interface User {
        boolean hasAccess();
    }

    private class DefaultUser implements User {
        public String first;
        public String last;

        public boolean hasAccess() {
            return false;
        }
    }

    private class SuperUser implements User {

        @Override
        public boolean hasAccess() {
            return true;
        }
    }
}
