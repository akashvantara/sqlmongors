use sqlparser::ast::{
    BinaryOperator, Expr, Ident, ObjectName, Offset, OrderByExpr, Select, SelectItem, SetExpr,
    Statement, TableFactor, TableWithJoins, Top, Value,
};

use mongodb::bson::{doc, Document};

#[allow(warnings)]
#[derive(Default, Debug)]
pub struct SqlQueryOpts {
    pub sort_options: Option<Document>,
    pub find_options: Option<Document>,
    pub specific_cols: Option<Document>,
    pub top_rows: Option<TopRows>,
    pub tables: Vec<String>,
    pub group_by: Vec<String>,
    pub distinct: bool,
    pub limit: Option<i64>,
    pub skip: Option<u64>,
}

#[derive(Debug)]
pub enum TopRows {
    Percent(usize),
    Number(usize),
}

pub fn convert_sql_to_sqlopts(stmt: &Statement) -> SqlQueryOpts {
    let mut sort_options = doc! {};
    let mut specific_cols = doc! {};
    let mut find_options = doc! {};
    let mut sql_query_opts = SqlQueryOpts::default();

    match stmt {
        Statement::Query(query) => {
            let set_expr = query.to_owned();

            match *set_expr.body {
                SetExpr::Table(table) => match *table {
                    _ => todo!(),
                },
                SetExpr::Select(select) => match *select {
                    Select {
                        distinct,
                        top,
                        projection,
                        into: _,
                        from,
                        lateral_views: _,
                        selection,
                        group_by,
                        cluster_by: _,
                        distribute_by: _,
                        sort_by: _,
                        having: _,
                        qualify: _,
                    } => {
                        // DISTINCT
                        sql_query_opts.distinct = distinct;

                        // TOP
                        if let Some(Top {
                            with_ties: _,
                            percent,
                            quantity,
                        }) = top
                        {
                            if let Some(Expr::Value(Value::Number(num, _null))) = quantity {
                                let num = num.parse().expect("Top val should have been number");
                                sql_query_opts.top_rows = if percent {
                                    Some(TopRows::Percent(num))
                                } else {
                                    Some(TopRows::Number(num))
                                };
                            }
                        } else {
                            sql_query_opts.top_rows = None;
                        }

                        // FROM
                        for f in from {
                            if let TableWithJoins {
                                relation:
                                    TableFactor::Table {
                                        name: ObjectName(table_vec),
                                        alias: _,
                                        args: _,
                                        with_hints: _,
                                    },
                                joins: _,
                            } = f
                            {
                                sql_query_opts.tables =
                                    table_vec.iter().map(|v| v.value.to_owned()).collect();
                            }
                        }

                        // SELECTION
                        if let Some(expr) = selection {
                            find_options = convert_to_doc(expr, doc! {});
                        }

                        // GROUP BY
                        for group in group_by {
                            if let Expr::Identifier(Ident {
                                value,
                                quote_style: _,
                            }) = group
                            {
                                sql_query_opts.group_by.push(value.to_owned());
                            }
                        }

                        // PROJECTION (Custom cols names to fetch)
                        for cols in projection {
                            match cols {
                                SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {}
                                SelectItem::UnnamedExpr(Expr::Identifier(Ident {
                                    value,
                                    quote_style: _,
                                })) => {
                                    specific_cols.insert(value, 1);
                                }
                                SelectItem::ExprWithAlias { expr: _, alias: _ } => {}
                                _ => {
                                    todo!("Non supported types");
                                }
                            }
                        }

                        // SORT BY
                        /*
                         * for expr in sort_by {
                         * }
                         */

                        // HAVING
                    }
                },
                SetExpr::Query(query) => match *query {
                    _ => todo!(),
                },
                _ => {
                    eprintln!("Can't parse modification stmt");
                }
            };

            // LIMIT 10 -> .limit()
            if let Some(Expr::Value(Value::Number(num, _null))) = set_expr.limit {
                sql_query_opts.limit = if let Ok(n) = num.parse() {
                    Some(n)
                } else {
                    None
                };
            }

            // ORDER BY ->  .sort({})
            for order in set_expr.order_by.iter() {
                if let OrderByExpr {
                    expr:
                        Expr::Identifier(Ident {
                            value,
                            quote_style: _,
                        }),
                    asc: Some(b),
                    nulls_first: _,
                } = order
                {
                    sort_options.insert(value.clone(), if *b { 1 } else { -1 });
                }
            }

            // OFFSET -> .skip(offset)
            if let Some(Offset {
                value: Expr::Value(Value::Number(num, _null)),
                rows: _,
            }) = set_expr.offset
            {
                sql_query_opts.skip = if let Ok(n) = num.parse() {
                    Some(n)
                } else {
                    None
                };
            }

            // FETCH, WITH isn't implemented yet
        }
        _ => {
            eprintln!("Error can't parse this SQL query ");
        }
    };

    sql_query_opts.sort_options = if sort_options.len() > 0 {
        Some(sort_options)
    } else {
        None
    };

    sql_query_opts.find_options = if find_options.len() > 0 {
        Some(find_options)
    } else {
        None
    };

    sql_query_opts.specific_cols = if specific_cols.len() > 0 {
        Some(specific_cols)
    } else {
        None
    };

    sql_query_opts
}

// Traverses the expr tree and adds all the nodes into MongoDB document
fn convert_to_doc(expr: Expr, mut document: Document) -> Document {
    let Expr::BinaryOp { op, left, right } = expr else { todo!() };

    if BinaryOperator::And == op {
        let left_doc = convert_to_doc(*left, Document::new());
        let right_doc = convert_to_doc(*right, Document::new());
        let vec_doc = vec![left_doc, right_doc];
        let mut ret_doc = Document::new();
        ret_doc.insert("$and", vec_doc);
        ret_doc
        /*let document = convert_to_doc(*left, document);
        let document = convert_to_doc(*right, document);
        document*/
    } else if BinaryOperator::Or == op {
        let left_doc = convert_to_doc(*left, Document::new());
        let right_doc = convert_to_doc(*right, Document::new());
        let vec_doc = vec![left_doc, right_doc];
        let mut ret_doc = Document::new();
        ret_doc.insert("$or", vec_doc);
        ret_doc
    } else {
        let bop = match op {
            BinaryOperator::Gt => "$gt",
            BinaryOperator::GtEq => "$gte",
            BinaryOperator::Lt => "$lt",
            BinaryOperator::LtEq => "$lte",
            BinaryOperator::Eq => "$eq",
            BinaryOperator::NotEq => "$ne",
            _ => "",
        };

        let left = left.to_string();
        if let Expr::Value(Value::SingleQuotedString(s)) = *right {
            if op == BinaryOperator::NotEq {
                document.insert(left, doc! {bop: s});
            } else {
                document.insert(left, s);
            }
        } else {
            let mut val = doc! {};
            let right: i64 = if let Ok(n) = right.to_string().parse() {
                n
            } else {
                0
            };
            val.insert(bop, right);
            document.insert(left, val);
        }

        document
    }
}
