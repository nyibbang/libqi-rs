use proc_macro2::TokenStream;
use quote::ToTokens;
use syn::{
    parse::{Parse, ParseStream},
    AttrStyle, Attribute, Expr, ExprLit, ItemTrait, Lit, LitStr, Meta, MetaNameValue, Result,
    TraitItem, TraitItemFn,
};

#[derive(Debug)]
pub(super) struct ObjectTrait {
    trait_item: ItemTrait,
    name: String,
    methods: Vec<Method>,
    signals: Vec<Signal>,
    properties: Vec<Property>,
    description: Vec<LitStr>,
}

impl ToTokens for ObjectTrait {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        self.trait_item.to_tokens(tokens);
        // nothing
    }
}

impl Parse for ObjectTrait {
    fn parse(input: ParseStream) -> Result<Self> {
        let mut trait_item = ItemTrait::parse(input)?;

        let description = trait_item
            .attrs
            .iter()
            .filter_map(attribute_outer_doc)
            .collect();

        let items_len = trait_item.items.len();
        let mut methods = Vec::with_capacity(items_len);
        let mut signals = Vec::with_capacity(items_len);
        let mut properties = Vec::with_capacity(items_len);

        for item in &mut trait_item.items {
            if let Some(method) = Method::from_item(item) {
                methods.push(method)
            } else if let Some(signal) = Signal::from_item(item) {
                signals.push(signal)
            } else if let Some(property) = Property::from_item(item) {
                properties.push(property)
            }
        }

        Ok(Self {
            name: trait_item.ident.to_string(),
            trait_item,
            methods,
            signals,
            properties,
            description,
        })
    }
}

#[derive(Debug)]
struct Method {
    func: TraitItemFn,
}

impl Method {
    fn from_item(item: &mut TraitItem) -> Option<Self> {
        trait_fn_item(item, "method").map(|func| Self { func })
    }
}

#[derive(Debug)]
struct Signal {
    func: TraitItemFn,
}

impl Signal {
    fn from_item(item: &mut TraitItem) -> Option<Self> {
        trait_fn_item(item, "signal").map(|func| Self { func })
    }
}

#[derive(Debug)]
struct Property {
    func: TraitItemFn,
}

impl Property {
    fn from_item(item: &mut TraitItem) -> Option<Self> {
        trait_fn_item(item, "property").map(|func| Self { func })
    }
}

fn attribute_outer_doc(attr: &Attribute) -> Option<LitStr> {
    if attr.style != AttrStyle::Outer {
        return None;
    }
    let MetaNameValue { path, value, .. } = attr.meta.require_name_value().ok()?;
    if !path.is_ident("doc") {
        return None;
    }
    let doc_str = match value {
        Expr::Lit(ExprLit {
            lit: Lit::Str(s), ..
        }) => s,
        _ => return None,
    };

    Some(doc_str.clone())
}

fn trait_fn_item(item: &mut TraitItem, tag: &str) -> Option<TraitItemFn> {
    let func = match item {
        TraitItem::Fn(f) => f,
        _ => return None,
    };

    let methods_attrs = func
        .attrs
        .extract_if(.., |attr| is_member_tag_attribute(attr, tag));
    if methods_attrs.count() == 0 {
        return None;
    }

    Some(func.clone())
}

fn is_member_tag_attribute(attr: &Attribute, ty: &str) -> bool {
    if attr.style != AttrStyle::Outer {
        return false;
    }

    let path = &attr.meta.path();
    if path.segments.len() != 2 {
        return false;
    }
    path.segments.iter().map(|seg| &seg.ident).eq(["qi", ty])
}
