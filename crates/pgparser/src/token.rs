//! Lexical tokens for the SP2 SQL slice.

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Token {
    Ident(String),
    Keyword(Keyword),
    IntLit(String),
    StringLit(String),
    LParen,
    RParen,
    Comma,
    Semicolon,
    Star,
    Plus,
    Minus,
    Slash,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Param(u32),
    Eof,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Keyword {
    Create,
    Table,
    Drop,
    Insert,
    Into,
    Values,
    Select,
    From,
    Where,
    Order,
    By,
    Asc,
    Desc,
    Limit,
    And,
    Or,
    Not,
    True,
    False,
    Null,
    As,
    // SP4: transaction control + DML
    Begin,
    Start,
    Transaction,
    Commit,
    End,
    Rollback,
    Abort,
    Update,
    Set,
    Delete,
    Isolation,
    Level,
    Read,
    Committed,
    Repeatable,
    // SP6: row-level locking
    For,
    Share,
    // SP27: aggregates + grouping
    Group,
    Having,
    Distinct,
    All,
    // SP28: predicate + conditional expression breadth
    Is,
    In,
    Between,
    Like,
    Ilike,
    Case,
    When,
    Then,
    Else,
    Offset,
}

impl Keyword {
    pub fn from_word(w: &str) -> Option<Keyword> {
        Some(match w {
            "create" => Keyword::Create,
            "table" => Keyword::Table,
            "drop" => Keyword::Drop,
            "insert" => Keyword::Insert,
            "into" => Keyword::Into,
            "values" => Keyword::Values,
            "select" => Keyword::Select,
            "from" => Keyword::From,
            "where" => Keyword::Where,
            "order" => Keyword::Order,
            "by" => Keyword::By,
            "asc" => Keyword::Asc,
            "desc" => Keyword::Desc,
            "limit" => Keyword::Limit,
            "and" => Keyword::And,
            "or" => Keyword::Or,
            "not" => Keyword::Not,
            "true" => Keyword::True,
            "false" => Keyword::False,
            "null" => Keyword::Null,
            "as" => Keyword::As,
            // SP4: transaction control + DML
            "begin" => Keyword::Begin,
            "start" => Keyword::Start,
            "transaction" => Keyword::Transaction,
            "commit" => Keyword::Commit,
            "end" => Keyword::End,
            "rollback" => Keyword::Rollback,
            "abort" => Keyword::Abort,
            "update" => Keyword::Update,
            "set" => Keyword::Set,
            "delete" => Keyword::Delete,
            "isolation" => Keyword::Isolation,
            "level" => Keyword::Level,
            "read" => Keyword::Read,
            "committed" => Keyword::Committed,
            "repeatable" => Keyword::Repeatable,
            // SP6: row-level locking
            "for" => Keyword::For,
            "share" => Keyword::Share,
            // SP27: aggregates + grouping
            "group" => Keyword::Group,
            "having" => Keyword::Having,
            "distinct" => Keyword::Distinct,
            "all" => Keyword::All,
            // SP28: predicate + conditional expression breadth
            "is" => Keyword::Is,
            "in" => Keyword::In,
            "between" => Keyword::Between,
            "like" => Keyword::Like,
            "ilike" => Keyword::Ilike,
            "case" => Keyword::Case,
            "when" => Keyword::When,
            "then" => Keyword::Then,
            "else" => Keyword::Else,
            "offset" => Keyword::Offset,
            _ => return None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_word_round_trips_every_keyword() {
        // Every keyword must map from its lowercase spelling; a dropped arm would
        // silently demote that word to an identifier (e.g. `ASC` parsed as a column).
        let pairs: &[(&str, Keyword)] = &[
            ("create", Keyword::Create),
            ("table", Keyword::Table),
            ("drop", Keyword::Drop),
            ("insert", Keyword::Insert),
            ("into", Keyword::Into),
            ("values", Keyword::Values),
            ("select", Keyword::Select),
            ("from", Keyword::From),
            ("where", Keyword::Where),
            ("order", Keyword::Order),
            ("by", Keyword::By),
            ("asc", Keyword::Asc),
            ("desc", Keyword::Desc),
            ("limit", Keyword::Limit),
            ("and", Keyword::And),
            ("or", Keyword::Or),
            ("not", Keyword::Not),
            ("true", Keyword::True),
            ("false", Keyword::False),
            ("null", Keyword::Null),
            ("as", Keyword::As),
            ("begin", Keyword::Begin),
            ("start", Keyword::Start),
            ("transaction", Keyword::Transaction),
            ("commit", Keyword::Commit),
            ("end", Keyword::End),
            ("rollback", Keyword::Rollback),
            ("abort", Keyword::Abort),
            ("update", Keyword::Update),
            ("set", Keyword::Set),
            ("delete", Keyword::Delete),
            ("isolation", Keyword::Isolation),
            ("level", Keyword::Level),
            ("read", Keyword::Read),
            ("committed", Keyword::Committed),
            ("repeatable", Keyword::Repeatable),
            ("for", Keyword::For),
            ("share", Keyword::Share),
            ("group", Keyword::Group),
            ("having", Keyword::Having),
            ("distinct", Keyword::Distinct),
            ("all", Keyword::All),
        ];
        for (word, kw) in pairs {
            assert_eq!(Keyword::from_word(word), Some(*kw), "from_word({word:?})");
        }
        // A non-keyword is an identifier, not a keyword.
        assert_eq!(Keyword::from_word("widget"), None);
        assert_eq!(Keyword::from_word("ascending"), None);
    }
}
