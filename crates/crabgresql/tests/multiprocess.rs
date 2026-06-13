mod harness;
use harness::Cluster;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bringup_elects_leader_and_serves_sql() {
    let c = Cluster::spawn(3).await;
    let leader = c.wait_for_leader().await;
    let client = c.pg(leader).await;
    client
        .simple_query("CREATE TABLE t (id int4)")
        .await
        .expect("create table");
    client
        .simple_query("INSERT INTO t VALUES (1)")
        .await
        .expect("insert");
    let rows = client
        .simple_query("SELECT id FROM t")
        .await
        .expect("select");
    let n = rows
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(n, 1, "leader serves SQL over the real cluster");
}
