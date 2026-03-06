use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

#[tokio::test]
async fn geosearch_near_poles() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Add points near the Mercator limit (~85.05°N)
    client
        .command(&["GEOADD", "mygeo", "0", "85.0", "northpole"])
        .await;
    client
        .command(&["GEOADD", "mygeo", "10", "84.9", "nearby"])
        .await;

    // Search near the Mercator pole limit
    let resp = client
        .command(&[
            "GEOSEARCH",
            "mygeo",
            "FROMLONLAT",
            "0",
            "85.0",
            "BYRADIUS",
            "500",
            "km",
            "ASC",
        ])
        .await;
    let members = extract_bulk_strings(&resp);
    assert!(
        members.contains(&"northpole".to_string()),
        "should find member near Mercator limit"
    );
    assert!(
        members.contains(&"nearby".to_string()),
        "should find nearby member near pole"
    );
}

#[tokio::test]
async fn geosearch_across_antimeridian() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Points on either side of the antimeridian (180/-180 longitude)
    client
        .command(&["GEOADD", "mygeo", "179.9", "0", "east"])
        .await;
    client
        .command(&["GEOADD", "mygeo", "-179.9", "0", "west"])
        .await;

    // Search from near the antimeridian - should find both points
    let resp = client
        .command(&[
            "GEOSEARCH",
            "mygeo",
            "FROMLONLAT",
            "180",
            "0",
            "BYRADIUS",
            "500",
            "km",
            "ASC",
        ])
        .await;
    let members = extract_bulk_strings(&resp);
    assert!(
        members.contains(&"east".to_string()),
        "should find member east of antimeridian"
    );
    assert!(
        members.contains(&"west".to_string()),
        "should find member west of antimeridian"
    );
}

#[tokio::test]
async fn geosearch_basic_ordering() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Add points at varying distances
    client
        .command(&["GEOADD", "mygeo", "0", "0", "origin"])
        .await;
    client.command(&["GEOADD", "mygeo", "1", "0", "near"]).await;
    client.command(&["GEOADD", "mygeo", "5", "0", "far"]).await;

    // ASC ordering should return closest first
    let resp = client
        .command(&[
            "GEOSEARCH",
            "mygeo",
            "FROMLONLAT",
            "0",
            "0",
            "BYRADIUS",
            "1000",
            "km",
            "ASC",
        ])
        .await;
    let members = extract_bulk_strings(&resp);
    assert_eq!(members[0], "origin", "closest member should be first");
    assert_eq!(members[1], "near", "second closest should be second");
    assert_eq!(members[2], "far", "farthest should be last");
}
