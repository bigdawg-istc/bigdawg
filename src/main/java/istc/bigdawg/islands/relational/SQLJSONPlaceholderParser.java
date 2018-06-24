package istc.bigdawg.islands.relational;

import istc.bigdawg.exceptions.QueryParsingException;
import java.util.*;

public class SQLJSONPlaceholderParser {
    private final static String placeholderPrefix = "BIGDAWG_PLACEHOLDER";
    private static Map<String, Placeholder> placeholders = new HashMap<>();
    private static int placeholderIndex = 0;

    private final static String[] operators = {
        "->>", // The ordering of these matter to the parser (as -> will match ->>)
//        "#>>",
//        "#>",
    };

    private static enum TokenType {
        STRING,
        QUOTED_STRING,
        OPERATOR_JSON_OBJ,
        OPERATOR_JSON_OBJ_TEXT,
        PLACEHOLDER
    }

    private static class Placeholder {
        String operator;
        String rightToken;
    }

    private static class Token {
        String token;
        TokenType tokenType;
    }

    private static class Tokenizer implements Iterator<Token>
    {
        private final String sql;
        private final int len;
        private int idx = 0;

        private Tokenizer(String sql) {
            this.sql = sql;
            this.len = sql.length();
        }

        @Override
        public Token next() {
            StringBuilder stringBuilder = new StringBuilder();
            boolean inQuote = false;
            while(idx < len) {
                char curChar = sql.charAt(idx);
                idx += 1;
                if (!inQuote) {
                    boolean end = false;
                    switch(curChar) {
                        case ' ':
                        case '\t':
                        case '\n':
                            if (stringBuilder.length() > 0) {
                                end = true;
                                break;
                            }
                            continue;
                        case '\'':
                            inQuote = true;
                        case '-':
                            Character lookaheadChar1 = null;
                            if (idx < len) {
                                lookaheadChar1 = sql.charAt(idx);
                            }
                            Character lookaheadChar2 = null;
                            if (idx+ 1 < len) {
                                lookaheadChar2 = sql.charAt(idx + 1);
                            }

                            if (lookaheadChar1 != null && lookaheadChar1 == '>') {
                                if (stringBuilder.length() > 0) {
                                    end = true;
                                    idx -= 1;
                                    break;
                                }

                                if (lookaheadChar2 != null && lookaheadChar2 == '>') {
                                    Token token = new Token();
                                    idx += 2;
                                    token.token = "->>";
                                    token.tokenType = TokenType.OPERATOR_JSON_OBJ_TEXT;
                                    return token;
                                }

                                Token token = new Token();
                                idx += 1;
                                token.token = "->";
                                token.tokenType = TokenType.OPERATOR_JSON_OBJ;
                                return token;
                           }
                            break;
                    }
                    if (end) {
                        break;
                    }
                    stringBuilder.append(curChar);
                }
                else {
                    Character lookaheadChar = null;
                    if (idx < len) {
                        lookaheadChar = sql.charAt(idx);
                    }
                    stringBuilder.append(curChar);
                    if (curChar == '\'') {
                        // handle escaped quote
                        if (lookaheadChar != null && lookaheadChar == '\'') {
                            idx += 1;
                            continue;
                        }
                        break;
                    }
                }
            }
            Token token = new Token();
            token.token = stringBuilder.toString();
            token.tokenType = inQuote ? TokenType.QUOTED_STRING : TokenType.STRING;
            if (token.tokenType == TokenType.STRING) {
                if (token.token.startsWith(placeholderPrefix)) {
                    token.tokenType = TokenType.PLACEHOLDER;
                }
            }
            return token;
        }

        @Override
        public boolean hasNext() {
            if (idx < len) {

                // look ahead for a real character:
                int idx2 = idx;
                while (idx2 < len) {
                    char curChar = sql.charAt(idx2);
                    idx2++;
                    switch (curChar) {
                        case ' ':
                        case '\t':
                        case '\n':
                            continue;
                    }
                    return true;
                }
                return false;
            }
            return false;
        }
    }

    private static boolean possiblyContainsJSONOperator(String sql) {
        return sql.contains("->"); // will also match '->' but it's okay
    }

    public static boolean possiblyContainsOperator(String sql) {
        for (String operator: operators) {
            if (sql.contains(operator)) {
                return true;
            }
        }
        return false;
    }

    public static boolean containsPlaceholder(String sql) {
        return sql.contains(placeholderPrefix);
    }

    public static String substitutePlaceholders(String sql) throws QueryParsingException {
        Tokenizer tokenizer = new Tokenizer(sql);
        StringJoiner stringJoiner = new StringJoiner(" ");
        boolean placeholderSubstitution = false;
        while (tokenizer.hasNext()) {
            Token token = tokenizer.next();
            if (token.tokenType == TokenType.OPERATOR_JSON_OBJ) {
                if (!tokenizer.hasNext()) {
                    throw new QueryParsingException("Expecting next token after -> in sql: " + sql);
                }

                Token nextToken = tokenizer.next();

                if (placeholders.containsKey(nextToken.token)) {
                    // placeholder
                    Placeholder placeholder = placeholders.get(nextToken.token);
                    stringJoiner.add(placeholder.operator);
                    stringJoiner.add(placeholder.rightToken);
                    if (!placeholderSubstitution) {
                        placeholderSubstitution = true;
                    }
                }
                else {
                    stringJoiner.add(token.token);
                    stringJoiner.add(nextToken.token);
                }
            }
            else {
                stringJoiner.add(token.token);
            }
        }

        if (!placeholderSubstitution) {
            return sql;
        }
        return stringJoiner.toString();
    }

    public static String transformJSONQuery(String sql) throws QueryParsingException {
        Tokenizer tokenizer = new Tokenizer(sql);

        Vector<String> finalTokens = new Vector<>();
        while (tokenizer.hasNext()) {
            Token token = tokenizer.next();
            if (token.tokenType == TokenType.QUOTED_STRING) {
                finalTokens.add(token.token);
                continue;
            }

            String finalToken = token.token;

            if (token.tokenType == TokenType.OPERATOR_JSON_OBJ_TEXT) {
                if (!tokenizer.hasNext()) {
                    throw new QueryParsingException("There should be a next token after " + finalToken);
                }
                Token nextToken = tokenizer.next();
                if (nextToken.tokenType != TokenType.QUOTED_STRING) {
                    throw new QueryParsingException("Having a non quoted string after a JSON ->> operator is not supported: " + nextToken.token);
                }

                placeholderIndex += 1;
                String placeholderStr = "'" + placeholderPrefix + placeholderIndex + "'";
                Placeholder placeholder = new Placeholder();
                placeholder.operator = "->>";
                placeholder.rightToken = nextToken.token;
                finalToken = "-> " + placeholderStr;
                placeholders.put(placeholderStr, placeholder);
            }
            finalTokens.add(finalToken);
        }
        StringJoiner stringJoiner = new StringJoiner(" ");
        for(String strToken: finalTokens) {
            stringJoiner.add(strToken);
        }
        return stringJoiner.toString();
    }
}
