package com.willmadison.legacycodekatas.examples.wrapping;

public class Undecorated {


    private class User {
        public String first;
        public String last;

        public boolean hasAccess() {
            return false;
        }
    }
}
