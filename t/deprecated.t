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

sub cleanup_childrens {
    my ($path) = @_;
    foreach my $znode ($zkh->get_children($path)) {
      cleanup($path . '/' . $znode);
    }
}

sub cleanup {
    my ($path) = @_;
    cleanup_childrens($path);
    $zkh->delete($path);
}

cleanup('/test_upstream');

create('/test_upstream');
create('/test_upstream/simple');
create('/test_upstream/simple/nodes');
create('/test_upstream/simple/nodes/127.0.0.1','{"port":1234}');
create('/test_upstream/simple/nodes/127.0.0.2','{"port":1234}');
create('/test_upstream/simple/nodes/127.0.0.3','{"port":1234}');

add_block_preprocessor(sub {
    my $block = shift;
    if ($block->name =~ /STEP 4/) {
        create('/test_upstream/simple/nodes/127.0.0.4','{"port":1234}');
        create('/test_upstream/simple/nodes/127.0.0.5','{"port":1234}');
        $zkh->delete('/test_upstream/simple/nodes/127.0.0.2');
        $zkh->delete('/test_upstream/simple/nodes/127.0.0.3');
    }
});

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

    upstream simple {
      zone shm_simple 128k;
      zookeeper_sync_path /test_upstream/simple/nodes;
      zookeeper_sync_file simple.peers;
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

    location = /sleep {
      echo_sleep $arg_delay;
    }

--- request
    GET /test
--- wait: 3


=== STEP 2: Check list
--- request
    GET /list
--- response_body_like eval
qr/\[{"name":"simple","lock":"","params_tag":"\@params","filter":""}\]/


=== STEP 3: Check peers
--- request
GET /dynamic?upstream=simple
--- response_body_like eval
qr/(server 127\.0\.0\.[123]:1234 addr=127\.0\.0\.[123]:1234;\n){3}/


=== STEP 4: Update & recheck
--- request eval
[
"GET /sleep?delay=11",
"GET /dynamic?upstream=simple"
]
--- response_body_like eval
[
"",
qr/(server 127\.0\.0\.[145]:1234 addr=127\.0\.0\.[145]:1234;
){3}/
]
--- timeout: 12



