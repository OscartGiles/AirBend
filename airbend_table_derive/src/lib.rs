use proc_macro2::TokenStream;
use quote::quote;
use syn::{parenthesized, parse::Parser, parse_macro_input, Data, DeriveInput, LitStr, Meta};

#[proc_macro_derive(AirbendTable, attributes(airbend_table, airbend_col))]
pub fn derive_airbend_table(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let name = &input.ident;
    let mut table_name: Option<LitStr> = None;
    for attr in &input.attrs {
        if attr.path().is_ident("airbend_table") {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("table_name") {
                    table_name = Some(meta.value().unwrap().parse().unwrap());
                    Ok(())
                } else {
                    Err(meta.error("unsupported table property"))
                }
            })
            .unwrap();
        }
    }

    if table_name.is_none() {
        panic!("You must provide table_name. Try adding #[airbend_table(table_name = \"my_table_name\")]");
    }

    let Data::Struct(struct_data) = input.data else {
        unimplemented!("AirbendTable can only be derived for structs");
    };

    let fields = struct_data.fields.iter().map(|field| {
        let mut ignore_field: bool = true;
        let mut col_name: Option<LitStr> = None;
        let mut col_dtype: Option<LitStr> = None;
        let Some(ident) = &field.ident else {
            unimplemented!("AirbendTable does not support tuple structs");
        };
        for inner_attr in &field.attrs {
            if inner_attr.path().is_ident("airbend_col") {
                ignore_field = false;
                inner_attr
                    .parse_nested_meta(|meta| {
                        if meta.path.is_ident("name") {
                            col_name = Some(meta.value().unwrap().parse().unwrap());
                            Ok(())
                        } else if meta.path.is_ident("dtype") {
                            col_dtype = Some(meta.value().unwrap().parse().unwrap());
                            Ok(())
                        } else {
                            Err(meta.error("unsupported table property"))
                        }
                    })
                    .unwrap();
            }
        }

        let resolved_col_name = if let Some(col_name_lit_str) = col_name {
            col_name_lit_str.value()
        } else {
            ident.to_string()
        };

        if ignore_field {
            (None, None)
        } else {
            (
                Some(quote!(airbend_table::Field {
                    name: #resolved_col_name,
                    data_type: #col_dtype,
                    nullable: true
                })),
                Some(quote!(
                    self.#ident.into()
                )),
            )
        }
    });

    let (included_field_types, to_rows): (Vec<_>, Vec<_>) =
        fields.filter(|i| i.0.is_some()).unzip();

    // Build the output, possibly using quasi-quotation
    let expanded = quote! {

        impl airbend_table::Table for #name {
             fn name() -> &'static str {
                 #table_name
             }

             fn schema() -> Vec<airbend_table::Field> {
                 vec![
                     #(#included_field_types),*
                 ]
             }
             fn to_row(self) -> Vec<airbend_table::InsertValue> {
                 vec![
                      #(#to_rows),*
                 ]
             }

        }

    };

    proc_macro::TokenStream::from(expanded)
}
