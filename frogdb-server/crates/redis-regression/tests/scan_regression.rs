use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

#[tokio::test]
async fn sscan_full_iteration_returns_all_members() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Add members to a set
    for i in 0..20 {
        client
            .command(&["SADD", "myset", &format!("member{i}")])
            .await;
    }

    // Iterate through full scan
    let mut cursor = "0".to_string();
    let mut all_members = Vec::new();
    loop {
        let resp = client.command(&["SSCAN", "myset", &cursor]).await;
        let arr = unwrap_array(resp);
        assert_eq!(arr.len(), 2);

        cursor = String::from_utf8(unwrap_bulk(&arr[0]).to_vec()).unwrap();
        let members = extract_bulk_strings(&arr[1]);
        all_members.extend(members);

        if cursor == "0" {
            break;
        }
    }

    all_members.sort();
    all_members.dedup();
    assert_eq!(all_members.len(), 20);
}

#[tokio::test]
async fn hscan_full_iteration_returns_all_fields() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for i in 0..20 {
        client
            .command(&["HSET", "myhash", &format!("field{i}"), &format!("val{i}")])
            .await;
    }

    let mut cursor = "0".to_string();
    let mut all_fields = Vec::new();
    loop {
        let resp = client.command(&["HSCAN", "myhash", &cursor]).await;
        let arr = unwrap_array(resp);
        assert_eq!(arr.len(), 2);

        cursor = String::from_utf8(unwrap_bulk(&arr[0]).to_vec()).unwrap();
        let field_vals = extract_bulk_strings(&arr[1]);
        // HSCAN returns field,value pairs — collect fields (even indices)
        for (i, s) in field_vals.iter().enumerate() {
            if i % 2 == 0 {
                all_fields.push(s.clone());
            }
        }

        if cursor == "0" {
            break;
        }
    }

    all_fields.sort();
    all_fields.dedup();
    assert_eq!(all_fields.len(), 20);
}

#[tokio::test]
async fn zscan_full_iteration_returns_all_members() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for i in 0..20 {
        client
            .command(&["ZADD", "myzset", &format!("{i}"), &format!("member{i}")])
            .await;
    }

    let mut cursor = "0".to_string();
    let mut all_members = Vec::new();
    loop {
        let resp = client.command(&["ZSCAN", "myzset", &cursor]).await;
        let arr = unwrap_array(resp);
        assert_eq!(arr.len(), 2);

        cursor = String::from_utf8(unwrap_bulk(&arr[0]).to_vec()).unwrap();
        let member_scores = extract_bulk_strings(&arr[1]);
        // ZSCAN returns member,score pairs — collect members (even indices)
        for (i, s) in member_scores.iter().enumerate() {
            if i % 2 == 0 {
                all_members.push(s.clone());
            }
        }

        if cursor == "0" {
            break;
        }
    }

    all_members.sort();
    all_members.dedup();
    assert_eq!(all_members.len(), 20);
}
