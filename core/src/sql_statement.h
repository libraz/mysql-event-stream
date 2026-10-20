#ifndef MES_CORE_SRC_SQL_STATEMENT_H_
#define MES_CORE_SRC_SQL_STATEMENT_H_

#include <cctype>
#include <cstddef>
#include <cstring>

namespace mes {

inline bool IsSqlWhitespace(char ch) {
  return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '\v' || ch == '\f';
}

/** Skip MySQL leading whitespace/comments and expose executable version comments. */
inline bool SkipLeadingSqlTrivia(const char* stmt, size_t stmt_len, size_t* pos) {
  for (;;) {
    while (*pos < stmt_len && IsSqlWhitespace(stmt[*pos])) ++(*pos);
    if (*pos >= stmt_len) return false;

    if (stmt[*pos] == '#') {
      while (*pos < stmt_len && stmt[*pos] != '\n' && stmt[*pos] != '\r') ++(*pos);
      continue;
    }

    if (stmt_len - *pos >= 2 && stmt[*pos] == '-' && stmt[*pos + 1] == '-' &&
        (stmt_len - *pos == 2 || IsSqlWhitespace(stmt[*pos + 2]))) {
      *pos += 2;
      while (*pos < stmt_len && stmt[*pos] != '\n' && stmt[*pos] != '\r') ++(*pos);
      continue;
    }

    if (stmt_len - *pos >= 2 && stmt[*pos] == '/' && stmt[*pos + 1] == '*') {
      const bool mysql_version_comment = stmt_len - *pos >= 3 && stmt[*pos + 2] == '!';
      const bool mariadb_version_comment = stmt_len - *pos >= 4 &&
                                           (stmt[*pos + 2] == 'M' || stmt[*pos + 2] == 'm') &&
                                           stmt[*pos + 3] == '!';
      if (mysql_version_comment || mariadb_version_comment) {
        *pos += mysql_version_comment ? 3 : 4;
        while (*pos < stmt_len && std::isdigit(static_cast<unsigned char>(stmt[*pos]))) ++(*pos);
        while (*pos < stmt_len && IsSqlWhitespace(stmt[*pos])) ++(*pos);
        return *pos < stmt_len;
      }

      const size_t comment_start = *pos;
      *pos += 2;
      bool closed = false;
      while (stmt_len - *pos >= 2) {
        if (stmt[*pos] == '*' && stmt[*pos + 1] == '/') {
          *pos += 2;
          closed = true;
          break;
        }
        ++(*pos);
      }
      if (!closed) {
        *pos = comment_start;
        return false;
      }
      continue;
    }

    return true;
  }
}

inline bool IsSqlIdentifierChar(char ch) {
  const auto uch = static_cast<unsigned char>(ch);
  return std::isalnum(uch) || ch == '_' || ch == '$';
}

/** True when @p keyword (upper case) starts at @p pos and ends on a word boundary. */
inline bool KeywordAt(const char* stmt, size_t stmt_len, size_t pos, const char* keyword) {
  const size_t keyword_len = std::strlen(keyword);
  if (stmt_len - pos < keyword_len) return false;
  for (size_t i = 0; i < keyword_len; ++i) {
    if (std::toupper(static_cast<unsigned char>(stmt[pos + i])) != keyword[i]) return false;
  }
  return stmt_len - pos == keyword_len || !IsSqlIdentifierChar(stmt[pos + keyword_len]);
}

/**
 * @brief Advance @p pos past a MariaDB `SET STATEMENT var=value, ... FOR` prefix.
 *
 * @return true when the prefix was present and @p pos now follows its FOR.
 */
inline bool SkipSetStatementPrefix(const char* stmt, size_t stmt_len, size_t* pos) {
  if (!KeywordAt(stmt, stmt_len, *pos, "SET")) return false;
  size_t cur = *pos + 3;
  if (!SkipLeadingSqlTrivia(stmt, stmt_len, &cur) || !KeywordAt(stmt, stmt_len, cur, "STATEMENT")) {
    return false;
  }
  cur += 9;
  int depth = 0;
  while (cur < stmt_len) {
    const char ch = stmt[cur];
    if (ch == '\'' || ch == '"' || ch == '`') {
      ++cur;
      while (cur < stmt_len && stmt[cur] != ch) cur += (stmt[cur] == '\\' && ch != '`') ? 2 : 1;
      ++cur;
    } else if (ch == '#' || ch == '/' || ch == '-') {
      size_t after = cur;
      if (!SkipLeadingSqlTrivia(stmt, stmt_len, &after)) return false;
      cur = after > cur ? after : cur + 1;
    } else if (ch == '(') {
      ++depth;
      ++cur;
    } else if (ch == ')') {
      --depth;
      ++cur;
    } else if (IsSqlIdentifierChar(ch)) {
      const size_t word_start = cur;
      while (cur < stmt_len && IsSqlIdentifierChar(stmt[cur])) ++cur;
      if (depth == 0 && cur - word_start == 3 && KeywordAt(stmt, stmt_len, word_start, "FOR")) {
        *pos = cur;
        return true;
      }
    } else {
      ++cur;
    }
  }
  return false;
}

}  // namespace mes

#endif  // MES_CORE_SRC_SQL_STATEMENT_H_
