#include <ngx_core.h>

#include <ngx_http.h>
#include <zookeeper/zookeeper.h>


#include "ngx_dynamic_upstream_module.h"
#include "ngx_zookeeper_upstream.h"


static void *
ngx_http_zookeeper_upstream_create_main_conf(ngx_conf_t *cf);


static void *
ngx_http_zookeeper_upstream_create_srv_conf(ngx_conf_t *cf);


static char *
ngx_http_zookeeper_upstream_log_level(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);


static ngx_int_t
ngx_http_zookeeper_upstream_post_conf(ngx_conf_t *cf);


ngx_int_t
ngx_http_zookeeper_upstream_init_worker(ngx_cycle_t *cycle);


void
ngx_http_zookeeper_upstream_exit_worker(ngx_cycle_t *cycle);


static char *
ngx_http_zookeeper_upstream_params(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);


typedef struct
{
    ngx_str_t    hosts;
    ngx_int_t    timeout;
    ZooLogLevel  log_level;
} ngx_http_zookeeper_upstream_main_conf_t;


typedef struct
{
    ngx_str_t  path;
    ngx_str_t  lock;
    ngx_str_t  lock_path;
    ngx_str_t  file;
    ngx_str_t  params_tag;
    ngx_str_t  filter;

    ngx_dynamic_upstream_op_t  defaults;
} ngx_http_zookeeper_upstream_srv_conf_t;


static char *
ngx_create_upsync_file(ngx_conf_t *cf, void *post, void *data);
static ngx_conf_post_t  ngx_upsync_file_post = {
    ngx_create_upsync_file
};


static ngx_conf_num_bounds_t  ngx_http_zookeeper_check_timeout = {
    ngx_conf_check_num_bounds,
    1, 60000
};

static ngx_command_t ngx_http_zookeeper_upstream_commands[] = {

    { ngx_string("zookeeper_upstream"),
      NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_str_slot,
      NGX_HTTP_MAIN_CONF_OFFSET,
      offsetof(ngx_http_zookeeper_upstream_main_conf_t, hosts),
      NULL },

    { ngx_string("zookeeper_upstream_log_level"),
      NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1,
      ngx_http_zookeeper_upstream_log_level,
      0,
      0,
      NULL },

    { ngx_string("zookeeper_upstream_recv_timeout"),
      NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_num_slot,
      NGX_HTTP_MAIN_CONF_OFFSET,
      offsetof(ngx_http_zookeeper_upstream_main_conf_t, timeout),
      &ngx_http_zookeeper_check_timeout },

    { ngx_string("zookeeper_sync_path"),
      NGX_HTTP_UPS_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_str_slot,
      NGX_HTTP_SRV_CONF_OFFSET,
      offsetof(ngx_http_zookeeper_upstream_srv_conf_t, path),
      NULL },

    { ngx_string("zookeeper_sync_lock"),
      NGX_HTTP_UPS_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_str_slot,
      NGX_HTTP_SRV_CONF_OFFSET,
      offsetof(ngx_http_zookeeper_upstream_srv_conf_t, lock),
      NULL },

    { ngx_string("zookeeper_sync_file"),
      NGX_HTTP_UPS_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_str_slot,
      NGX_HTTP_SRV_CONF_OFFSET,
      offsetof(ngx_http_zookeeper_upstream_srv_conf_t, file),
      &ngx_upsync_file_post },

    { ngx_string("zookeeper_sync_params"),
      NGX_HTTP_UPS_CONF|NGX_CONF_2MORE,
      ngx_http_zookeeper_upstream_params,
      NGX_HTTP_SRV_CONF_OFFSET,
      0,
      NULL },

    { ngx_string("zookeeper_sync_filter"),
      NGX_HTTP_UPS_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_str_slot,
      NGX_HTTP_SRV_CONF_OFFSET,
      offsetof(ngx_http_zookeeper_upstream_srv_conf_t, filter),
      NULL },

    ngx_null_command
};


static ngx_http_module_t ngx_zookeeper_upstream_ctx = {
    NULL,                                         /* preconfiguration */
    ngx_http_zookeeper_upstream_post_conf,        /* postconfiguration */
    ngx_http_zookeeper_upstream_create_main_conf, /* create main */
    NULL,                                         /* init main */
    ngx_http_zookeeper_upstream_create_srv_conf,  /* create server */
    NULL,                                         /* merge server */
    NULL,                                         /* create location */
    NULL                                          /* merge location */
};


ngx_module_t ngx_zookeeper_upstream_module = {
    NGX_MODULE_V1,
    &ngx_zookeeper_upstream_ctx,              /* module context */
    ngx_http_zookeeper_upstream_commands,     /* module directives */
    NGX_HTTP_MODULE,                          /* module type */
    NULL,                                     /* init master */
    NULL,                                     /* init module */
    ngx_http_zookeeper_upstream_init_worker,  /* init process */
    NULL,                                     /* init thread */
    NULL,                                     /* exit thread */
    ngx_http_zookeeper_upstream_exit_worker,  /* exit process */
    NULL,                                     /* exit master */
    NGX_MODULE_V1_PADDING
};


static void *
ngx_http_zookeeper_upstream_create_main_conf(ngx_conf_t *cf)
{
    ngx_http_zookeeper_upstream_main_conf_t  *zmcf;

    zmcf = ngx_pcalloc(cf->pool,
        sizeof(ngx_http_zookeeper_upstream_main_conf_t));
    if (zmcf == NULL)
        return NULL;

    zmcf->log_level = ZOO_LOG_LEVEL_ERROR;
    zmcf->timeout = NGX_CONF_UNSET;

    return zmcf;
}


static void *
ngx_http_zookeeper_upstream_create_srv_conf(ngx_conf_t *cf)
{
    ngx_http_zookeeper_upstream_srv_conf_t  *zscf;

    zscf = ngx_pcalloc(cf->pool,
        sizeof(ngx_http_zookeeper_upstream_srv_conf_t));
    if (zscf == NULL)
        return NULL;

    ngx_str_set(&zscf->params_tag, "@params");

    return zscf;
}


static void
str_trim(ngx_str_t *s)
{
    while (s->len != 0 && isspace(*s->data)) {
        s->data++;
        s->len--;
    }
    while (s->len != 0 && isspace(*(s->data + s->len - 1)))
        s->len--;
}


static ngx_flag_t
str_eq(ngx_str_t s1, ngx_str_t s2)
{
    return ngx_memn2cmp(s1.data, s2.data, s1.len, s2.len) == 0;
}


static void
parse_token(ngx_dynamic_upstream_op_t *op,
    ngx_str_t token)
{
    ngx_str_t  k, v;

    static const ngx_str_t MAX_FAILS    = ngx_string("max_fails");
    static const ngx_str_t FAIL_TIMEOUT = ngx_string("fail_timeout");
    static const ngx_str_t WEIGHT       = ngx_string("weight");
    static const ngx_str_t BACKUP       = ngx_string("backup");
    static const ngx_str_t DOWN         = ngx_string("down");
#if defined(nginx_version) && (nginx_version >= 1011005)
    static const ngx_str_t MAX_CONNS    = ngx_string("max_conns");
#endif

    k.data = token.data;
    v.data = ngx_strlchr(token.data, token.data + token.len, '=');

    if (v.data != NULL) {
        k.len = v.data - k.data;
        v.data++;
        v.len = token.len - k.len - 1;
    } else {
        k.len = token.len;
        v.len = 0;
    }

    str_trim(&k);
    if (v.len)
        str_trim(&v);

    if (op->server.data == NULL) {

        op->server = k;
        return;
    }

    ngx_strlow(k.data, k.data, k.len);
    
    if (str_eq(BACKUP, k)) {

        op->backup = 1;
    } else if (str_eq(DOWN, k)) {

        op->down = 1;
        op->op_param |= NGX_DYNAMIC_UPSTEAM_OP_PARAM_DOWN;
    } else if (str_eq(MAX_FAILS, k)) {

        op->max_fails = ngx_atoi(v.data, v.len);
        op->op_param |= NGX_DYNAMIC_UPSTEAM_OP_PARAM_MAX_FAILS;
    } else if (str_eq(FAIL_TIMEOUT, k)) {

        op->fail_timeout = ngx_parse_time(&v, 0);
        op->op_param |= NGX_DYNAMIC_UPSTEAM_OP_PARAM_FAIL_TIMEOUT;
    } else if (str_eq(WEIGHT, k)) {

        op->weight = ngx_atoi(v.data, v.len);
        op->op_param |= NGX_DYNAMIC_UPSTEAM_OP_PARAM_WEIGHT;
    }
#if defined(nginx_version) && (nginx_version >= 1011005)
    else if (str_eq(MAX_CONNS, k)) {

        op->max_conns = ngx_atoi(v.data, v.len);
        op->op_param |= NGX_DYNAMIC_UPSTEAM_OP_PARAM_MAX_CONNS;
    }
#endif
}


static void
parse_server(ngx_dynamic_upstream_op_t *op,
    ngx_str_t *server)
{
    /*
     * format:
     *    attr:port weight=1 max_conns=1 max_fails=1 fail_timeout=1s backup
     */

    u_char     *s1, *s2;
    ngx_str_t   token;

    for (s1 = s2 = server->data;
         s2 < server->data + server->len;
         s2++) {

        if (isspace(*s2) || s2 == server->data + server->len - 1) {

            token.data = s1;
            token.len = s2 - s1;
            if (s2 == server->data + server->len - 1)
                token.len++;

            parse_token(op, token);

            while (s2 < server->data + server->len && isspace(*s2))
                s2++;
            s1 = s2;
        }
    }
}


static char *
ngx_http_zookeeper_upstream_params(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf)
{
    ngx_http_zookeeper_upstream_srv_conf_t  *zscf = conf;
    ngx_str_t                               *elts;
    ngx_uint_t                               j;

    ngx_str_set(&zscf->defaults.server, "defaults");

    elts = cf->args->elts;

    zscf->params_tag = elts[1];

    for (j = 2; j < cf->args->nelts; j++)
        parse_token(&zscf->defaults, elts[j]);

    return NGX_CONF_OK;
}


static char *
ngx_http_zookeeper_upstream_log_level(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf)
{
    ngx_http_zookeeper_upstream_main_conf_t  *zmcf = conf;
    ngx_str_t                                 log_level;

    static const ngx_str_t LOG_ERR   = ngx_string("error");
    static const ngx_str_t LOG_INFO  = ngx_string("warn");
    static const ngx_str_t LOG_WARN  = ngx_string("info");
    static const ngx_str_t LOG_DEBUG = ngx_string("debug");
    
    log_level = ((ngx_str_t *) cf->args->elts)[1];
    ngx_strlow(log_level.data, log_level.data, log_level.len);

    if (str_eq(LOG_ERR, log_level))

        zmcf->log_level = ZOO_LOG_LEVEL_ERROR;
    else if (str_eq(LOG_WARN, log_level))

        zmcf->log_level = ZOO_LOG_LEVEL_WARN;
    else if (str_eq(LOG_INFO, log_level))

        zmcf->log_level = ZOO_LOG_LEVEL_INFO;
    else if (str_eq(LOG_DEBUG, log_level))

        zmcf->log_level = ZOO_LOG_LEVEL_DEBUG;
    else {

        ngx_log_error(NGX_LOG_ERR, cf->log, 0,
            "invalid zookeeper_log_level value (error, warn, info, debug)");
        return NGX_CONF_ERROR;
    }

    zoo_set_debug_level(zmcf->log_level);

    return NGX_CONF_OK;
}


typedef struct {
    ngx_http_upstream_srv_conf_t            *uscf;
    ngx_http_zookeeper_upstream_srv_conf_t  *zscf;
    int                                      epoch;
    ngx_flag_t                               busy;
} ngx_zookeeper_srv_conf_t;


typedef struct
{
    zhandle_t                 *handle;
    ngx_flag_t                 connected;
    const clientid_t          *client_id;
    ngx_flag_t                 expired;
    int                        epoch;
    ngx_uint_t                 len;
    ngx_zookeeper_srv_conf_t  *cfg;
} zookeeper_t;


static zookeeper_t zoo = {
    .handle    = NULL,
    .connected = 0,
    .client_id = NULL,
    .expired   = 1,
    .epoch     = 1,
    .cfg       = NULL
};


ngx_flag_t
ngx_zookeeper_upstream_connected()
{
    return zoo.connected;
}


int
ngx_zookeeper_upstream_epoch()
{
    return zoo.epoch;
}


void *
ngx_zookeeper_upstream_handle()
{
    return zoo.handle;
}


static void
ngx_log_message(const char *s)
{
    ngx_log_error(NGX_LOG_DEBUG, ngx_cycle->log, 0, s);
}


static void
initialize(volatile ngx_cycle_t *cycle);


static ngx_int_t
ngx_zookeeper_sync_upstreams();


static void
ngx_zookeeper_sync_handler(ngx_event_t *ev)
{
    if (zoo.expired) {

        if (zoo.handle != NULL) {

            zookeeper_close(zoo.handle);
            zoo.handle = NULL;
            zoo.client_id = 0;
        }

        initialize(ngx_cycle);
    }

    if (!zoo.connected)
        goto settimer;

    ngx_zookeeper_sync_upstreams();

settimer:

    if (ngx_exiting || ngx_terminate || ngx_quit)
        // cleanup
        ngx_memset(ev, 0, sizeof(ngx_event_t));
    else
        ngx_add_timer(ev, 10000);
}


static void
session_watcher(zhandle_t *zh,
                int type,
                int state,
                const char *path,
                void* context);


static ngx_connection_t dumb_conn = {
    .fd = -1
};
static ngx_event_t sync_ev = {
    .handler = ngx_zookeeper_sync_handler,
    .data = &dumb_conn,
    .log = NULL
};


static void
initialize(volatile ngx_cycle_t *cycle)
{
    ngx_http_zookeeper_upstream_main_conf_t *zmcf;

    zmcf = ngx_http_cycle_get_module_main_conf(cycle,
        ngx_zookeeper_upstream_module);

    zoo.handle = zookeeper_init2((const char *) zmcf->hosts.data,
                                 session_watcher,
                                 zmcf->timeout,
                                 zoo.client_id,
                                 0,
                                 0,
                                 ngx_log_message);

    if (zoo.handle == NULL) {

        u_char err[1024];

        ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0,
                      "Zookeeper upstream: error create zookeeper handle: %s",
                      ngx_strerror(errno, err, sizeof(err)));

        return;
    }

    zoo.expired = 0;

    ngx_log_error(NGX_LOG_INFO, cycle->log, 0,
                  "Zookeeper upstream: connecting ...");
}


static void
session_watcher(zhandle_t *zh,
                int type,
                int state,
                const char *path,
                void* ctx)
{
    if (type == ZOO_SESSION_EVENT) {

        if (state == ZOO_CONNECTED_STATE) {

            zoo.connected = 1;
            zoo.client_id = zoo_client_id(zh);
            zoo.epoch += 1;

            ngx_log_error(NGX_LOG_INFO, ngx_cycle->log, 0,
                          "Zookeeper upstream: received a connected event");

        } else if (state == ZOO_CONNECTING_STATE) {

            if (zoo.connected) {
                ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0,
                              "Zookeeper upstream: disconnected");
            }

            zoo.connected = 0;

        } else if (state == ZOO_EXPIRED_SESSION_STATE) {

            if (zh != NULL) {

                ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0,
                              "Zookeeper upstream: session has been expired");

                zoo.connected = 0;
                zoo.expired = 1;
            }
        }
    }
}


static FILE *
state_open(ngx_str_t *state_file, const char *mode)
{
    FILE  *f;

    f = fopen((const char *) state_file->data, mode);
    if (f == NULL)
        ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "can't open file: %V",
                      state_file);

    return f;
}


static char *
ngx_create_upsync_file(ngx_conf_t *cf, void *post, void *data)
{
    ngx_str_t  *fname = data;
    FILE       *f;

    static const ngx_str_t
        default_server = ngx_string("server 0.0.0.0:1 down;");

    if (ngx_conf_full_name(cf->cycle, fname, 1) != NGX_OK)
        return NGX_CONF_ERROR;

    f = state_open(fname, "r");
    if (f != NULL) {
        fclose(f);
        return ngx_conf_include(cf, NULL, NULL);
    }

    f = state_open(fname, "w+");
    if (f == NULL)
        return NGX_CONF_ERROR;

    fwrite(default_server.data, default_server.len, 1, f);

    fclose(f);

    return ngx_conf_include(cf, NULL, NULL);
}


static void
ngx_zookeeper_upstream_save(ngx_zookeeper_srv_conf_t *cfg)
{
    ngx_http_upstream_rr_peer_t   *peer;
    ngx_http_upstream_rr_peers_t  *peers, *primary;
    ngx_uint_t                     j = 0;
    u_char                         srv[10240], *c;
    FILE                          *f;
    ngx_pool_t                    *pool;
    ngx_array_t                   *servers;
    ngx_str_t                     *server, *s;
    ngx_uint_t                     i;

    pool = ngx_create_pool(2048, ngx_cycle->log);
    if (pool == NULL) {
        ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0,
                      "Zookeeper upstream: no memory");
        return;
    }

    f = state_open(&cfg->zscf->file, "w+");
    if (f == NULL)
        return;

    primary = cfg->uscf->peer.data;

    ngx_rwlock_rlock(&primary->rwlock);

    servers = ngx_array_create(pool, 100, sizeof(ngx_str_t));
    if (servers == NULL)
        goto nomem;

    server = servers->elts;

    for (peers = primary;
         peers && j < 2;
         peers = peers->next, j++) {

        for (peer = peers->peer;
             peer;
             peer = peer->next) {

            for (i = 0; i < servers->nelts; i++)
                if (str_eq(peer->server, server[i]))
                    // already saved
                    break;

            if (i == servers->nelts) {
                s = ngx_array_push(servers);
                if (s == NULL)
                    goto nomem;
                *s = peer->server;
                c = ngx_snprintf(srv, 10240,
                    "server %V max_conns=%d max_fails=%d fail_timeout=%d "
                    "weight=%d",
                    &peer->server, peer->max_conns, peer->max_fails,
                    peer->fail_timeout, peer->weight);
                fwrite(srv, c - srv, 1, f);
                if (j == 1)
                    fwrite(" backup", 7, 1, f);
                fwrite(";\n", 2, 1, f);
            }
        }
    }

end:

    ngx_rwlock_unlock(&primary->rwlock);

    fclose(f);

    ngx_destroy_pool(pool);

    return;

nomem:

    ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0,
                  "Zookeeper upstream: no memory");
    goto end;
}


static ngx_int_t
ngx_http_zookeeper_upstream_post_conf(ngx_conf_t *cf)
{
    ngx_http_upstream_main_conf_t            *umcf;
    ngx_http_upstream_srv_conf_t            **uscf;
    ngx_http_zookeeper_upstream_main_conf_t  *zmcf;
    ngx_uint_t                                j;
    ngx_str_t                                *lock;

    umcf = ngx_http_conf_get_module_main_conf(cf,
        ngx_http_upstream_module);
    zmcf = ngx_http_conf_get_module_main_conf(cf,
        ngx_zookeeper_upstream_module);

    ngx_conf_init_value(zmcf->timeout, 10000);

    zoo.len = umcf->upstreams.nelts;
    zoo.cfg = ngx_pcalloc(cf->pool,
        sizeof(ngx_zookeeper_srv_conf_t) * umcf->upstreams.nelts);

    if (zoo.cfg == NULL) {
        ngx_log_error(NGX_LOG_INFO, ngx_cycle->log, 0,
            "Zookeeper upstream: no memory");
        return NGX_ERROR;
    }

    uscf = (ngx_http_upstream_srv_conf_t **) umcf->upstreams.elts;

    for (j = 0; j < umcf->upstreams.nelts; j++) {

        zoo.cfg[j].uscf = uscf[j];
        zoo.cfg[j].zscf = ngx_http_conf_upstream_srv_conf(uscf[j],
            ngx_zookeeper_upstream_module);

        if (zoo.cfg[j].zscf->path.data != NULL && uscf[j]->shm_zone != NULL) {

            if (zoo.cfg[j].zscf->lock.data != NULL) {

                zoo.cfg[j].zscf->lock_path = zoo.cfg[j].zscf->lock;
                lock = &zoo.cfg[j].zscf->lock;

                lock->len = lock->len + cf->cycle->hostname.len + 1;
                lock->data = ngx_pcalloc(cf->pool, lock->len + 1);
                if (lock->data == NULL) {
                    ngx_log_error(NGX_LOG_INFO, ngx_cycle->log, 0,
                        "Zookeeper upstream: no memory");
                    return NGX_ERROR;
                }

                ngx_snprintf(lock->data, lock->len + 1,
                    "%V/%V", &zoo.cfg[j].zscf->lock_path, &cf->cycle->hostname);
            }

            ngx_log_error(NGX_LOG_INFO, ngx_cycle->log, 0,
                          "Zookeeper upstream: [%V] sync on", &uscf[j]->host);
        } else
            zoo.cfg[j].zscf = NULL;
    }

    return NGX_OK;
}


ngx_int_t
ngx_http_zookeeper_upstream_init_worker(ngx_cycle_t *cycle)
{
    ngx_http_zookeeper_upstream_main_conf_t *zmcf;

    zmcf = ngx_http_cycle_get_module_main_conf(cycle,
        ngx_zookeeper_upstream_module);

    if (ngx_process != NGX_PROCESS_WORKER && ngx_process != NGX_PROCESS_SINGLE)
        return NGX_OK;

    ngx_log_error(NGX_LOG_INFO, cycle->log, 0,
                  "Zookeeper upstream: initialized");

    if (zmcf == NULL || zmcf->hosts.len == 0)
        return NGX_OK;

    initialize(cycle);

    sync_ev.log = cycle->log;

    ngx_add_timer(&sync_ev, 2000);

    return NGX_OK;
}


void
ngx_http_zookeeper_upstream_exit_worker(ngx_cycle_t *cycle)
{
    if (sync_ev.log != NULL) {
        ngx_del_timer(&sync_ev);
        ngx_memset(&sync_ev, 0, sizeof(ngx_event_t));
    }

    if (zoo.handle == NULL)
        return;

    zookeeper_close(zoo.handle);

    zoo.handle = NULL;

    ngx_log_error(NGX_LOG_INFO, cycle->log, 0,
                  "Zookeeper upstream: destroyed");
}


static void
ngx_zookeeper_op_defaults(ngx_dynamic_upstream_op_t *op,
    ngx_str_t *upstream, ngx_str_t *server, ngx_str_t *name, int operation,
    ngx_dynamic_upstream_op_t *defaults)
{
    ngx_memcpy(op, defaults, sizeof(ngx_dynamic_upstream_op_t));
    ngx_str_null(&op->server);

    op->op = operation;
    op->err = "unknown";

    op->status = NGX_HTTP_OK;
    op->down = 1;

    op->op_param |= NGX_DYNAMIC_UPSTEAM_OP_PARAM_RESOLVE_SYNC;

    parse_server(op, server);

    if (!op->down) {

        op->op_param |= NGX_DYNAMIC_UPSTEAM_OP_PARAM_UP;
        op->up = 1;
    }

    if (name != NULL) {
        op->name.data = name->data;
        op->name.len = name->len;
    }

    op->upstream.data = upstream->data;
    op->upstream.len = upstream->len;

    server->len = op->server.len;
}

static void
ngx_zookeeper_op_defaults_locked(ngx_dynamic_upstream_op_t *op,
    ngx_str_t *upstream, ngx_str_t *server, ngx_str_t *name, int operation,
    ngx_dynamic_upstream_op_t *defaults)
{
    ngx_zookeeper_op_defaults(op, upstream, server, name,
        operation, defaults);
    op->no_lock = 1;
}


static void
ngx_zookeeper_remove_obsoleted(ngx_zookeeper_srv_conf_t *cfg,
    const struct String_vector *names)
{
    ngx_http_upstream_rr_peer_t   *peer;
    ngx_http_upstream_rr_peers_t  *peers, *primary;
    ngx_uint_t                     j = 0;
    ngx_dynamic_upstream_op_t      op;
    char                         **elts;
    int32_t                        i;
    ngx_str_t                      server;

    static ngx_str_t  noaddr = ngx_string("0.0.0.0:1");

    elts = names->data;

    primary = cfg->uscf->peer.data;

    ngx_rwlock_wlock(&primary->rwlock);

    for (peers = primary;
         peers && j < 2;
         peers = peers->next, j++) {

        for (peer = peers->peer;
             peer;
             peer = peer->next) {

            for (i = 0; i < names->count; i++) {

                server.data = (u_char *) elts[i];
                server.len = ngx_strlen(server.data);

                if (str_eq(peer->server, server))
                    break;
            }

            if (i == names->count) {

again:

                ngx_zookeeper_op_defaults_locked(&op, &cfg->uscf->host,
                    &peer->server, &peer->name, NGX_DYNAMIC_UPSTEAM_OP_REMOVE,
                    &cfg->zscf->defaults);

                if (ngx_dynamic_upstream_op(ngx_cycle->log, &op, cfg->uscf)
                        == NGX_ERROR) {

                    if (op.status == NGX_HTTP_BAD_REQUEST) {

                        ngx_zookeeper_op_defaults_locked(&op,
                            &cfg->uscf->host, &noaddr, &noaddr,
                            NGX_DYNAMIC_UPSTEAM_OP_ADD, &cfg->zscf->defaults);

                        ngx_dynamic_upstream_op(ngx_cycle->log, &op,
                            cfg->uscf);

                        if (ngx_strcmp(noaddr.data, peer->name.data) != 0)
                            goto again;
                    } else
                        ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0,
                                      "Zookeeper upstream: [%V] %s",
                                      &op.upstream, op.err);
                }
            }
        }
    }

    ngx_rwlock_unlock(&primary->rwlock);
}


static ngx_int_t
ngx_zookeeper_sync_update(ngx_zookeeper_srv_conf_t *cfg);


static void
ngx_zookeeper_sync_watch(zhandle_t *zh, int type,
    int state, const char *path, void *ctx)
{
    ngx_zookeeper_srv_conf_t  *cfg = ctx;

    if (type == ZOO_CHILD_EVENT
        || type == ZOO_CHANGED_EVENT
        || type == ZOO_DELETED_EVENT) {

        ngx_log_error(NGX_LOG_INFO, ngx_cycle->log, 0,
                      "Zookeeper upstream: [%V] changed", &cfg->uscf->host);
        cfg->epoch = 0;
        ngx_msleep(100);
        ngx_zookeeper_sync_update(cfg);
    }
}


typedef struct {
    u_char                     node[1024];
    u_char                     server[256];
    ngx_zookeeper_srv_conf_t  *cfg;
    ngx_flag_t                 last;
    ngx_pool_t                *pool;
    struct String_vector       names;
    int32_t                    index;
} ngx_zookeeper_node_ctx_t;


static ngx_array_t *
parse_body(ngx_pool_t *pool, const char *body, int len)
{
    char         *s1, *s2;
    ngx_array_t  *a;
    ngx_str_t    *name;

    a = ngx_array_create(pool, 2, sizeof(ngx_str_t));
    if (a == NULL)
        return NULL;

    while (len > 0 && isspace(*body)) {
        body++;
        len--;
    }

    for (s1 = s2 = (char *) body;
         s2 < body + len;
         s2++) {

        if (*s2 == LF || s2 == body + len - 1) {

            name = ngx_array_push(a);
            if (name == NULL)
                return NULL;

            name->data = (u_char *) s1;
            name->len = s2 - s1;

            if (*s2 == LF)
                *s2++ = 0;
            else if (s2 == body + len - 1)
                name->len++;

            while (s2 < body + len && isspace(*s2))
                s2++;
            s1 = s2;
        }
    }

    return a;
}


static void
ngx_zookeeper_sync_lock(int rc, const char *dummy, const void *ctx)
{
    ngx_zookeeper_srv_conf_t  *cfg = (ngx_zookeeper_srv_conf_t *) ctx;

    cfg->busy = 0;

    if (rc != ZOK && rc != ZNODEEXISTS) {
        ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0,
                      "Zookeeper upstream: [%V] failed to "
                      "register lock path=%V, %s",  &cfg->uscf->host,
                      &cfg->zscf->lock, zerror(rc));
        return;
    }

    ngx_log_error(NGX_LOG_INFO, ngx_cycle->log, 0,
                  "Zookeeper upstream: [%V] lock registered, path=%V",
                  &cfg->uscf->host, &cfg->zscf->lock);
}


static void
ngx_zookeeper_sync_upstream_host(int rc, const char *body, int len,
    const struct Stat *stat, const void *p)
{
    ngx_zookeeper_node_ctx_t  *ctx = (ngx_zookeeper_node_ctx_t *) p;
    ngx_dynamic_upstream_op_t  op;
    ngx_str_t                  server;
    ngx_array_t               *tags;
    ngx_str_t                 *tag;
    ngx_uint_t                 j;
    ngx_str_t                  filter, params;
    ngx_flag_t                 filtered;

    if (rc != ZOK) {

        ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0,
                      "Zookeeper upstream: [%V] get server, node=%s, %s",
                      &ctx->cfg->uscf->host, ctx->node, zerror(rc));
        goto end;
    }

    server.data = ctx->server;
    server.len = ngx_strlen(server.data);

    ngx_zookeeper_op_defaults(&op, &ctx->cfg->uscf->host, &server,
        NULL, NGX_DYNAMIC_UPSTEAM_OP_ADD, &ctx->cfg->zscf->defaults);

    tags = parse_body(ctx->pool, body, len);
    if (tags == NULL) {

        ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0,
                      "Zookeeper upstream: [%V] get server, node=%s, no memory",
                      &ctx->cfg->uscf->host, ctx->node);
        goto end;
    }

    params = ctx->cfg->zscf->params_tag;

    filter = ctx->cfg->zscf->filter;
    filtered = filter.data != NULL ? 0 : 1;

    tag = tags->elts;
    for (j = 0; j < tags->nelts; j++) {

        if (params.data != NULL
            && ngx_strncmp(params.data, tag[j].data, params.len) == 0) {

            parse_server(&op, tag + j);
            continue;
        }

        if (filter.data != NULL && str_eq(filter, tag[j]))
            filtered = 1;
    }

    if (!filtered) {
        ctx->names.data[ctx->index][0] = 0;
        goto skip;
    }

again:

    switch (ngx_dynamic_upstream_op(ngx_cycle->log, &op, ctx->cfg->uscf)) {

        case NGX_OK:
            if (op.status == NGX_HTTP_NOT_MODIFIED) {

                op.op = NGX_DYNAMIC_UPSTEAM_OP_PARAM;
                goto again;
            }
            break;

        case NGX_ERROR:
            if (op.status == NGX_HTTP_PRECONDITION_FAILED) {

                op.op = NGX_DYNAMIC_UPSTEAM_OP_REMOVE;

                if (ngx_dynamic_upstream_op(ngx_cycle->log, &op,
                        ctx->cfg->uscf) == NGX_OK) {

                    op.op = NGX_DYNAMIC_UPSTEAM_OP_ADD;
                    goto again;
                }
            }

        default:
            ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0,
                          "Zookeeper upstream: [%V] add server, %s",
                          &op.upstream, op.err);
            goto end;
    }

skip:

    ngx_zookeeper_remove_obsoleted(ctx->cfg, &ctx->names);

    if (ctx->last) {

        if (ctx->cfg->zscf->file.data != NULL)
            ngx_zookeeper_upstream_save(ctx->cfg);

        if (ctx->cfg->zscf->lock.data == NULL) {

            ctx->cfg->epoch = zoo.epoch;
            goto end;
        }

        rc = zoo_acreate(zoo.handle, (const char *) ctx->cfg->zscf->lock.data,
            "", 0, &ZOO_OPEN_ACL_UNSAFE, 0, ngx_zookeeper_sync_lock, ctx->cfg);
        if (rc != ZOK)
            ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0,
                          "Zookeeper upstream: [%V] failed to "
                          "register lock, %s", 
                          &ctx->cfg->uscf->host, zerror(rc));
    }

end:

    if (ctx->last)
        ctx->cfg->busy = 0;

    ngx_destroy_pool(ctx->pool);
}


static void
ngx_zookeeper_sync_upstream_childrens(int rc, const struct String_vector *names,
    const void *ctx)
{
    ngx_zookeeper_srv_conf_t  *cfg = (ngx_zookeeper_srv_conf_t *) ctx;
    int32_t                    j, i;
    ngx_str_t                  server;
    ngx_zookeeper_node_ctx_t  *gctx;
    ngx_pool_t                *pool;
    int                        len;

    if (rc != ZOK) {

        ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0,
                      "Zookeeper upstream: [%V] get nodes, %s",
                      &cfg->uscf->host, zerror(rc));
        cfg->busy = 0;
        return;
    }

    if (names->count == 0) {

        ngx_zookeeper_remove_obsoleted(cfg, names);

        ngx_log_error(NGX_LOG_INFO, ngx_cycle->log, 0,
                      "Zookeeper upstream: [%V] no nodes", &cfg->uscf->host);

        cfg->busy = 0;
        cfg->epoch = zoo.epoch;
        return;
    }

    for (j = 0; j < names->count; j++) {

        server.data = (u_char *) names->data[j];
        server.len = ngx_strlen(server.data);

        pool = ngx_create_pool(2048, ngx_cycle->log);
        if (pool == NULL)
            goto nomem;

        gctx = ngx_pcalloc(pool, sizeof(ngx_zookeeper_node_ctx_t));
        if (gctx == NULL)
            goto nomem;

        gctx->pool = pool;
        gctx->cfg = cfg;
        gctx->index = j;
        ngx_snprintf(gctx->server, sizeof(gctx->server), "%V", &server);
        gctx->last = j == names->count - 1;

        gctx->names.count = names->count;
        gctx->names.data = ngx_palloc(pool, names->count * sizeof(char *));
        if (gctx->names.data == NULL)
            goto nomem;
        for (i = 0; i < names->count; i++) {

            len = strlen(names->data[i]);
            gctx->names.data[i] = ngx_pcalloc(pool, len + 1);
            if (gctx->names.data[i] == NULL)
                goto nomem;
            ngx_memcpy(gctx->names.data[i], names->data[i], len);
        }

        ngx_snprintf(gctx->node, sizeof(gctx->node), "%V/%V",
            &cfg->zscf->path, &server);

        if (cfg->zscf->lock.data == NULL)
            rc = zoo_awget(zoo.handle, (const char *) gctx->node,
                ngx_zookeeper_sync_watch, cfg,
                ngx_zookeeper_sync_upstream_host, gctx);
        else
            rc = zoo_aget(zoo.handle, (const char *) gctx->node,
                0, ngx_zookeeper_sync_upstream_host, gctx);

        if (rc != ZOK) {
            ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0,
                          "Zookeeper upstream: [%V] get nodes, %s",
                          &cfg->uscf->host, zerror(rc));

            goto end;
        }

        continue;

nomem:

        ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0,
                      "Zookeeper upstream: [%V] get nodes, no memory",
                      &cfg->uscf->host);

end:

        cfg->busy = 0;
        if (pool != NULL)
            ngx_destroy_pool(pool);

        return;
    }
}


static ngx_int_t
ngx_zookeeper_sync_upstream(ngx_zookeeper_srv_conf_t *cfg)
{
    int rc;

    if (cfg->zscf->lock.data == NULL)
        rc = zoo_awget_children(zoo.handle, (const char *) cfg->zscf->path.data,
            ngx_zookeeper_sync_watch, cfg,
            ngx_zookeeper_sync_upstream_childrens, cfg);
    else
        rc = zoo_aget_children(zoo.handle, (const char *) cfg->zscf->path.data,
            0, ngx_zookeeper_sync_upstream_childrens, cfg);

    if (rc == ZOK)
        return NGX_OK;

    ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0,
                  "Zookeeper upstream: [%V] sync, %s",
                  &cfg->uscf->host, zerror(rc));

    cfg->busy = 0;

    return NGX_ERROR;
}


static void
ngx_zookeeper_sync_upstream_locked(int rc, const struct Stat *dummy,
    const void *ctx)
{
    ngx_zookeeper_srv_conf_t  *cfg = (ngx_zookeeper_srv_conf_t *) ctx;

    if (rc == ZNONODE) {

        ngx_zookeeper_sync_upstream(cfg);
        return;
    }

    if (rc == ZOK) {

        ngx_log_error(NGX_LOG_DEBUG, ngx_cycle->log, 0,
                      "Zookeeper upstream: [%V] locked, path=%V",
                      &cfg->uscf->host, &cfg->zscf->lock);

        cfg->epoch = zoo.epoch;
        cfg->busy = 0;
        return;
    }

    ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0,
                  "Zookeeper upstream: [%V] locked, path=%V, %s",
                  &cfg->uscf->host, &cfg->zscf->lock, zerror(rc));

    cfg->busy = 0;
}


static void
ensure_zpath_ready(int rc, const char *dummy, const void *ctx)
{
    ngx_str_t  *path = (ngx_str_t *) ctx;

    if (rc != ZOK && rc != ZNODEEXISTS)
        ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0,
                      "Zookeeper upstream: error create path: %V, %s",
                      path, zerror(rc));

    ngx_free(path->data);
    ngx_free(path);
}


static void
ensure_zpath(const ngx_str_t *path)
{
    u_char     *s2;
    ngx_str_t  *sub;

    for (s2 = path->data + 1;
         s2 <= path->data + path->len;
         s2++) {

        if (*s2 == '/' || *s2 == 0) {

            sub = ngx_calloc(sizeof(ngx_str_t), ngx_cycle->log);
            if (sub == NULL)
                goto nomem;

            sub->data = ngx_calloc(s2 - path->data + 1, ngx_cycle->log);
            if (sub->data == NULL)
                goto nomem;
            sub->len = s2 - path->data;
            ngx_memcpy(sub->data, path->data, sub->len);

            zoo_acreate(zoo.handle, (const char *) sub->data, "", 0,
                &ZOO_OPEN_ACL_UNSAFE, 0, ensure_zpath_ready, sub);
        }
    }

    return;

nomem:

    ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0,
                  "Zookeeper upstream: no memory");
}


static void
ensure_lock_path_ready(int rc, const struct Stat *dummy,
    const void *ctx)
{
    ngx_zookeeper_srv_conf_t  *cfg = (ngx_zookeeper_srv_conf_t *) ctx;

    if (rc == ZOK || rc == ZNODEEXISTS)
        goto cont;

    if (rc == ZNONODE) {

        cfg->busy = 0;
        return ensure_zpath(&cfg->zscf->lock_path);
    }

    ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0,
                  "Zookeeper upstream: error create path: %V, %s",
                  &cfg->zscf->lock_path, zerror(rc));

    cfg->busy = 0;
    return;

cont:

    rc = zoo_awexists(zoo.handle, (const char *) cfg->zscf->lock.data,
        ngx_zookeeper_sync_watch, cfg, ngx_zookeeper_sync_upstream_locked, cfg);

    if (rc != ZOK) {

        ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0,
                      "Zookeeper upstream: [%V] update, %s",
                      &cfg->uscf->host, zerror(rc));
        cfg->busy = 0;
    }
}


static ngx_int_t
ngx_zookeeper_sync_update(ngx_zookeeper_srv_conf_t *cfg)
{
    int rc;

    if (cfg->zscf == NULL)
        return NGX_OK;

    if (cfg->busy)
        return NGX_OK;

    if (cfg->epoch == zoo.epoch)
        return NGX_OK;

    cfg->busy = 1;

    if (cfg->zscf->lock.data == NULL)
        return ngx_zookeeper_sync_upstream(cfg);

    rc = zoo_aexists(zoo.handle, (const char *) cfg->zscf->lock_path.data,
        0, ensure_lock_path_ready, cfg);

    if (rc != ZOK) {
        ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0,
                      "Zookeeper upstream: [%V] update, %s",
                      &cfg->uscf->host, zerror(rc));
        cfg->busy = 0;
        return NGX_ERROR;
    }

    return NGX_OK;
}


static ngx_int_t
ngx_zookeeper_sync_upstreams()
{
    ngx_uint_t        j;
    ngx_core_conf_t  *ccf;

    ccf = (ngx_core_conf_t *) ngx_get_conf(ngx_cycle->conf_ctx,
                                           ngx_core_module);

    for (j = 0; j < zoo.len; j++)
        if (j % ccf->worker_processes == ngx_worker)
            ngx_zookeeper_sync_update(&zoo.cfg[j]);

    return NGX_OK;
}
