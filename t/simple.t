use Test::Nginx::Socket 'no_plan';
use Net::ZooKeeper qw(:node_flags :acls);

$ENV{TEST_NGINX_ZOOKEEPER_PORT} ||= 2181;

# prepare zoo data

my $zkh = Net::ZooKeeper->new('127.0.0.1:' . $ENV{TEST_NGINX_ZOOKEEPER_PORT});

sub create {
    my ($path, $val) = @_;
    $zkh->create($path, $val || '', 'acl' => ZOO_OPEN_ACL_UNSAFE) or
        die("unable to create node " . $path . ", err:" . $zkh->get_error());
}

sub cleanup {
    my ($path) = @_;
    foreach my $znode ($zkh->get_children($path)) {
      cleanup($path . '/' . $znode);
    }
    $zkh->delete($path);
}

cleanup('/test_upstream');

create('/test_upstream');
create('/test_upstream/simple1');
create('/test_upstream/simple1/nodes');
create('/test_upstream/simple2');
create('/test_upstream/simple2/nodes');
create('/test_upstream/simple1/nodes/127.0.0.1:5555');
create('/test_upstream/simple1/nodes/127.0.0.2:5555');
create('/test_upstream/simple2/nodes/127.0.0.3:4444');
create('/test_upstream/simple2/nodes/127.0.0.4:4444');
create('/test_upstream/.locks');
create('/test_upstream/.locks/simple2');

add_cleanup_handler(sub {
    cleanup('/test_upstream');
});

no_shuffle();
run_tests();

__DATA__

=== STEP 1: Init
--- http_config

    zookeeper_upstream                  127.0.0.1:$TEST_NGINX_ZOOKEEPER_PORT;
    zookeeper_upstream_log_level        debug;
    zookeeper_upstream_recv_timeout     5000;

    upstream simple1 {
      zone shm_simple1 128k;
      zookeeper_sync_path /test_upstream/simple1/nodes;
      zookeeper_sync_file simple1.peers;
    }

    upstream simple2 {
      zone shm_simple2 128k;
      zookeeper_sync_path /test_upstream/simple2/nodes;
      zookeeper_sync_lock /test_upstream/.locks/simple2;
      zookeeper_sync_file simple2.peers;
    }

    upstream xxx {
      zone shm_simple1 128k;
      server 0.0.0.0:1;
    }

    upstream yyy {
      server 0.0.0.0:1;
    }

--- config

    location = /test {
      return 200;
    }

    location = /list {
      zookeeper_sync_list;
    }

    location = /dynamic {
      dynamic_upstream;
    }

--- request
    GET /test
--- wait: 3


=== STEP 2: Check list
--- request
    GET /list
--- response_body_like eval
qr/\[\{"name":"simple1","lock":"","params_tag":"\@params","filter":""\},\r
\{"name":"simple2","lock":"\/test_upstream\/.locks\/simple2\/.+","params_tag":"\@params","filter":""\}\]/


=== STEP 3: Check peers
--- request eval
["GET /dynamic?upstream=simple1","GET /dynamic?upstream=simple2"]
--- response_body_like eval
[qr/server 127\.0\.0\.[12]:5555 addr=127\.0\.0\.[12]:5555;
server 127\.0\.0\.[12]:5555 addr=127\.0\.0\.[12]:5555;
/,
 qr/server 127\.0\.0\.[34]:4444 addr=127\.0\.0\.[34]:4444;
server 127\.0\.0\.[34]:4444 addr=127\.0\.0\.[34]:4444;
/]
