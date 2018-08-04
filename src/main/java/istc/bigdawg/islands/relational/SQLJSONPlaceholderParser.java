package istc.bigdawg.islands.relational;

import istc.bigdawg.exceptions.QueryParsingException;
import istc.bigdawg.utils.Tuple;

import java.util.*;

public class SQLJSONPlaceholderParser {
    private final static String placeholderPrefix = "BIGDAWG_PLACEHOLDER";
    private static Map<String, Placeholder> placeholders = new HashMap<>();
    private static int placeholderIndex = 0;

    private final static String[] operators = {
        "->",
        "->>", // The ordering of these matter to the parser (as -> will match ->>)
        "#>>",
        "#>",
    };

    static void resetIndex() {
        placeholderIndex = 0;
    }

    private enum TokenType {
        STRING,
        QUOTED_STRING,
        OPERATOR_JSON,
        PAREN_OPEN,
        PAREN_CLOSE,
        PLACEHOLDER
    }

    private static class Placeholder {
        Vector<Token> lhs;
        String operator;
        Vector<Token> rhs;
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
        private Token prevToken = null;
        private Token curToken = null;

        private Tokenizer(String sql) {
            this.sql = sql;
            this.len = sql.length();
        }

        public Token lookAhead() {
            final int curIdx = idx;
            final Token prevToken = this.prevToken;
            final Token token = next();

            // reset index
            idx = curIdx;
            curToken = this.prevToken;
            this.prevToken = prevToken;

            return token;
        }

        @Override
        public Token next() {
            StringBuilder stringBuilder = new StringBuilder();
            boolean inQuote = false;
            while(idx < len) {
                char curChar = sql.charAt(idx);
                idx += 1;
                Character lookaheadChar1 = null;
                Character lookaheadChar2 = null;
                Token token;
                if (!inQuote) {
                    boolean end = false;
                    switch (curChar) {
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
                            break;
                        case '(':
                            if (stringBuilder.length() > 0) {
                                end = true;
                                stringBuilder.append('(');
                                break;
                            }
                            token = new Token();
                            token.token = "(";
                            token.tokenType = TokenType.PAREN_OPEN;
                            prevToken = curToken;
                            curToken = token;
                            return token;
                        case ')':
                            if (stringBuilder.length() > 0) {
                                end = true;
                                idx--;
                                break;
                            }
                            token = new Token();
                            token.token = ")";
                            token.tokenType = TokenType.PAREN_CLOSE;
                            prevToken = curToken;
                            curToken = token;
                            return token;
                        case '-':
                        case '#':
                            if (idx < len) {
                                lookaheadChar1 = sql.charAt(idx);
                            }
                            if (idx + 1 < len) {
                                lookaheadChar2 = sql.charAt(idx + 1);
                            }

                            if (lookaheadChar1 != null && lookaheadChar1 == '>') {
                                if (stringBuilder.length() > 0) {
                                    end = true;
                                    idx -= 1;
                                    break;
                                }

                                if (lookaheadChar2 != null && lookaheadChar2 == '>') {
                                    token = new Token();
                                    idx += 2;
                                    token.token = curChar + ">>";
                                    token.tokenType = TokenType.OPERATOR_JSON;
                                    prevToken = curToken;
                                    curToken = token;
                                    return token;
                                }

                                token = new Token();
                                idx += 1;
                                token.token = curChar + ">";
                                token.tokenType = TokenType.OPERATOR_JSON;
                                prevToken = curToken;
                                curToken = token;
                                return token;
                            }
                            break;
                    }
                    if (end) {
                        break;
                    }
                    stringBuilder.append(curChar);
                } else {
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
            prevToken = curToken;
            curToken = token;
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

        public Token getPrevToken() {
            return prevToken;
        }
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

    private static void addPlaceholderTokens(StringJoiner stringJoiner, Placeholder placeholder) {
        for (Token token: placeholder.lhs) {
            if (placeholders.containsKey(token.token)) {
                addPlaceholderTokens(stringJoiner, placeholders.get(token.token));
            }
            else {
                stringJoiner.add(token.token);
            }
        }

        stringJoiner.add(placeholder.operator);

        for (Token token: placeholder.rhs) {
            if (placeholders.containsKey(token.token)) {
                addPlaceholderTokens(stringJoiner, placeholders.get(token.token));
            }
            else {
                stringJoiner.add(token.token);
            }
        }
    }

    public static String substitutePlaceholders(String sql) throws QueryParsingException {
        Tokenizer tokenizer = new Tokenizer(sql);
        StringJoiner stringJoiner = new StringJoiner(" ");
        boolean placeholderSubstitution = false;
        while (tokenizer.hasNext()) {
            Token token = tokenizer.next();
            if (token.tokenType == TokenType.QUOTED_STRING) {
                if (placeholders.containsKey(token.token)) {
                    // placeholder
                    Placeholder placeholder = placeholders.get(token.token);
                    addPlaceholderTokens(stringJoiner, placeholder);
                    if (!placeholderSubstitution) {
                        placeholderSubstitution = true;
                    }
                }
                else {
                    stringJoiner.add(token.token);
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

    private static Vector<Token> popLHS(Vector<Token> tokens) {
        int length = tokens.size();
        final Vector<Token> lhsTokens = new Vector<>();

        Token token = tokens.get(length - 1);
        tokens.remove(length - 1);
        lhsTokens.add(token);
        if (token.tokenType == TokenType.PAREN_CLOSE) {
            int count = 1;
            for (int i = length - 2; i >= 0 ; i--) {
                token = tokens.get(i);
                tokens.remove(i);

                if (token.tokenType == TokenType.PAREN_OPEN) {
                    count--;
                }
                else if (token.tokenType == TokenType.PAREN_CLOSE) {
                    count--;
                }
                lhsTokens.add(0, token);
                if (count == 0) {
                    break;
                }
            }
        }

        return lhsTokens;
    }

    private static Vector<Token> getRHS(Tokenizer tokenizer) {
        Vector<Token> tokens = new Vector<>();

        if(tokenizer.hasNext()) {
            Token token = tokenizer.next();
            tokens.add(token);
            if (token.tokenType == TokenType.PAREN_OPEN) {
                int count = 1;
                while (count > 0 && tokenizer.hasNext()) {
                    token = tokenizer.next();
                    tokens.add(token);
                    if (token.tokenType == TokenType.PAREN_OPEN) {
                        count--;
                    }
                    else if (token.tokenType == TokenType.PAREN_CLOSE) {
                        count--;
                    }
                }
            }
        }
        return tokens;
    }

    public static String transformJSONQuery(String sql) throws QueryParsingException {
        Tokenizer tokenizer = new Tokenizer(sql);

        Vector<Token> finalTokens = new Vector<>();
        while (tokenizer.hasNext()) {
            Token token = tokenizer.next();
            if (token.tokenType == TokenType.QUOTED_STRING) {
                finalTokens.add(token);
                continue;
            }

            if (token.tokenType == TokenType.OPERATOR_JSON) {
                if (!tokenizer.hasNext()) {
                    throw new QueryParsingException("There should be a next token after " + token.token);
                }

                if (finalTokens.size() == 0) {
                    throw new QueryParsingException("There should be a previous token before " + token.token);
                }

                // Now need to collapse LHS and RHS into a single thing.
                Vector<Token> lhs = popLHS(finalTokens);
                Vector<Token> rhs = getRHS(tokenizer);

                placeholderIndex += 1;
                String placeholderStr = "'" + placeholderPrefix + placeholderIndex + "'";
                Placeholder placeholder = new Placeholder();
                placeholder.lhs = lhs;
                placeholder.rhs = rhs;
                placeholder.operator = token.token;
                placeholders.put(placeholderStr, placeholder);
                Token placeholderToken = new Token();
                placeholderToken.token = placeholderStr;
                placeholderToken.tokenType = TokenType.QUOTED_STRING;
                finalTokens.add(placeholderToken);
            }
            else {
                finalTokens.add(token);
            }
        }
        StringJoiner stringJoiner = new StringJoiner(" ");
        for(Token token: finalTokens) {
            stringJoiner.add(token.token);
        }
        return stringJoiner.toString();
    }
}
