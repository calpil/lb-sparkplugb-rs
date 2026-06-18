//! Alias registry: the name ↔ alias ↔ datatype bindings established by a BIRTH.
//!
//! BIRTH metrics carry name + alias + datatype; later DATA/CMD metrics may carry
//! only an alias and omit the datatype (`tck-id-payloads-metric-datatype-not-req`).
//! Decoding such a payload requires recovering the datatype from this registry
//! (built from the birth). Unlike Tahu's `MetricDataTypeMap`, [`clear`](AliasRegistry::clear)
//! clears every map (Tahu's leaves the alias map populated — a known leak).

use std::collections::HashMap;

use crate::datatype::DataType;

/// A lookup key: either a metric name or an alias.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum MetricKey {
    /// Look up by metric name.
    Name(String),
    /// Look up by alias.
    Alias(u64),
}

/// Bidirectional name↔alias bindings plus a datatype index, scoped to one
/// Edge Node or Device.
#[derive(Clone, Debug, Default)]
pub struct AliasRegistry {
    name_to_alias: HashMap<String, u64>,
    alias_to_name: HashMap<u64, String>,
    dt_by_name: HashMap<String, DataType>,
    dt_by_alias: HashMap<u64, DataType>,
}

impl AliasRegistry {
    /// An empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Bind a metric's name (and optional alias) to its datatype, as declared in
    /// a BIRTH.
    pub fn bind(&mut self, name: &str, alias: Option<u64>, datatype: DataType) {
        self.dt_by_name.insert(name.to_owned(), datatype);
        if let Some(alias) = alias {
            self.name_to_alias.insert(name.to_owned(), alias);
            self.alias_to_name.insert(alias, name.to_owned());
            self.dt_by_alias.insert(alias, datatype);
        }
    }

    /// The datatype declared for `name`, if any.
    #[must_use]
    pub fn datatype_for_name(&self, name: &str) -> Option<DataType> {
        self.dt_by_name.get(name).copied()
    }

    /// The datatype declared for `alias`, if any.
    #[must_use]
    pub fn datatype_for_alias(&self, alias: u64) -> Option<DataType> {
        self.dt_by_alias.get(&alias).copied()
    }

    /// The datatype for a [`MetricKey`].
    #[must_use]
    pub fn datatype_for(&self, key: &MetricKey) -> Option<DataType> {
        match key {
            MetricKey::Name(n) => self.datatype_for_name(n),
            MetricKey::Alias(a) => self.datatype_for_alias(*a),
        }
    }

    /// The name bound to `alias`, if any.
    #[must_use]
    pub fn name_for_alias(&self, alias: u64) -> Option<&str> {
        self.alias_to_name.get(&alias).map(String::as_str)
    }

    /// The alias bound to `name`, if any.
    #[must_use]
    pub fn alias_for_name(&self, name: &str) -> Option<u64> {
        self.name_to_alias.get(name).copied()
    }

    /// Whether `alias` is already bound (to detect duplicate aliases in a birth).
    #[must_use]
    pub fn alias_exists(&self, alias: u64) -> bool {
        self.alias_to_name.contains_key(&alias)
    }

    /// Clear every binding (call before re-binding on a new birth).
    pub fn clear(&mut self) {
        self.name_to_alias.clear();
        self.alias_to_name.clear();
        self.dt_by_name.clear();
        self.dt_by_alias.clear();
    }
}
