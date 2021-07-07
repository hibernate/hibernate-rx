/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool.impl;

public class PostgresParameters extends Parameters {

    public static final PostgresParameters INSTANCE = new PostgresParameters();

    private PostgresParameters() {
    }

    public String process(String sql) {
        if (isProcessingNotRequired(sql)) {
            return sql;
        }
        return new Parser(sql).result();
    }

    /**
     * Limit and offset gets applied just before the execution of the query but because we know
     * how the string looks like for Postgres, it's faster to replace the last bit instead
     * of processing the whole query
     */
    public String processLimit(String sql, Object[] parameterArray, boolean hasOffset) {
        if (isProcessingNotRequired(sql)) {
            return sql;
        }

        // Replace 'limit ? offset ?' with the $ style parameters for PostgreSQL
        int index = hasOffset ? parameterArray.length - 1 : parameterArray.length;
        int pos = sql.indexOf(" limit ?");
        if (pos > -1) {
            String sqlProcessed = sql.substring(0, pos) + " limit $" + index++;
            if (hasOffset) {
                sqlProcessed += " offset $" + index;
            }
            return sqlProcessed;
        }

        return sql;
    }

    /**
     * Replace all JDBC-style {@code ?} parameters with Postgres-style
     * {@code $n} parameters in the given SQL string.
     */
    public String process(String sql, int parameterCount) {
        if (isProcessingNotRequired(sql)) {
            return sql;
        }
        return new Parser(sql, parameterCount).result();
    }

    private static class Parser {

        private boolean inString;
        private boolean inQuoted;
        private boolean inSqlComment;
        private boolean inCComment;
        private boolean escaped;
        private int count = 0;
        private StringBuilder result;
        private int previous;

        private Parser(String sql) {
            this(sql, 10);
        }

        private Parser(String sql, int parameterCount) {
            result = new StringBuilder(sql.length() + parameterCount);
            sql.codePoints().forEach(this::append);
        }

        private String result() {
            return result.toString();
        }

        private void append(int codePoint) {
            if (escaped) {
                escaped = false;
            } else {
                switch (codePoint) {
                    case '\\':
                        escaped = true;
                        break;
                    case '"':
                        if (!inString && !inSqlComment && !inCComment) inQuoted = !inQuoted;
                        break;
                    case '\'':
                        if (!inQuoted && !inSqlComment && !inCComment) inString = !inString;
                        break;
                    case '-':
                        if (!inQuoted && !inString && !inCComment && previous == '-') inSqlComment = true;
                        break;
                    case '\n':
                        inSqlComment = false;
                        break;
                    case '*':
                        if (!inQuoted && !inString && !inSqlComment && previous == '/') inCComment = true;
                        break;
                    case '/':
                        if (previous == '*') inCComment = false;
                        break;
                    //TODO: $$-quoted strings
                    case '?':
                        if (!inQuoted && !inString) {
                            result.append('$').append(++count);
                            previous = '?';
                            return;
                        }
                }
            }
            previous = codePoint;
            result.appendCodePoint(codePoint);
        }
    }
}
