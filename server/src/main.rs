//! Actix entrypoint — deliberately shaped like open-msupply's server crate:
//! POST /graphql, GET /graphql (GraphiQL), GET /graphql/ws (subscriptions).
//!
//! THREADING MODEL (the part that makes the demo work):
//! `HttpServer::workers(n)` spawns n OS threads, each running its own
//! CURRENT-THREAD tokio runtime (actix-rt). A WebSocket connection lives on
//! whichever worker accepted it, and `tokio::spawn` from inside a resolver
//! lands on that same worker's runtime. There is NO work stealing between
//! actix workers — they are isolated single-threaded runtimes, not a
//! multi-threaded tokio pool.
//!
//! Default is WORKERS=1 so everything shares one thread and starvation is
//! total. Try `WORKERS=4 cargo run` to see how extra workers HIDE the bug
//! (other connections survive) without fixing it (the naive subscription
//! still starves itself — its socket and its blocking task share a worker).

mod db;
mod graphql;

use actix_web::{guard, web, web::Data, App, HttpRequest, HttpResponse, HttpServer, Result};
use async_graphql::{http::GraphiQLSource, EmptyMutation, Schema};
use async_graphql_actix_web::{GraphQLRequest, GraphQLResponse, GraphQLSubscription};

use graphql::{DemoConfig, QueryRoot, SubscriptionRoot};

type DemoSchema = Schema<QueryRoot, EmptyMutation, SubscriptionRoot>;

async fn graphql_index(schema: Data<DemoSchema>, req: GraphQLRequest) -> GraphQLResponse {
    schema.execute(req.into_inner()).await.into()
}

async fn graphql_playground() -> Result<HttpResponse> {
    Ok(HttpResponse::Ok().content_type("text/html; charset=utf-8").body(
        GraphiQLSource::build()
            .endpoint("/graphql")
            .subscription_endpoint("/graphql/ws")
            .finish(),
    ))
}

async fn graphql_ws(
    schema: Data<DemoSchema>,
    req: HttpRequest,
    payload: web::Payload,
) -> Result<HttpResponse> {
    GraphQLSubscription::new(Schema::clone(&schema)).start(&req, payload)
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let workers: usize = std::env::var("WORKERS")
        .ok()
        .and_then(|w| w.parse().ok())
        .unwrap_or(1);

    let schema = Schema::build(QueryRoot, EmptyMutation, SubscriptionRoot)
        .data(DemoConfig { workers })
        .finish();

    println!("subscription-blocking-demo server");
    println!("  workers:  {workers}  (override with WORKERS=n)");
    println!("  graphiql: http://localhost:8088/graphql");
    println!("  ws:       ws://localhost:8088/graphql/ws");

    HttpServer::new(move || {
        App::new()
            .app_data(Data::new(schema.clone()))
            .service(
                web::resource("/graphql")
                    .guard(guard::Post())
                    .to(graphql_index),
            )
            .service(
                web::resource("/graphql")
                    .guard(guard::Get())
                    .to(graphql_playground),
            )
            .service(
                web::resource("/graphql/ws")
                    .guard(guard::Get())
                    .to(graphql_ws),
            )
    })
    .workers(workers)
    .bind(("127.0.0.1", 8088))?
    .run()
    .await
}
