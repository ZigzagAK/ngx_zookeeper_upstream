#include <ngx_core.h>

#include <ngx_http.h>
#include <zookeeper/zookeeper.h>


#include "ngx_dynamic_upstream_module.h"


static void *
ngx_http_zookeeper_upstream_create_main_conf(ngx_conf_t *cf);


static void *
ngx_http_zookeeper_upstream_create_srv_conf(ngx_conf_t *cf);


static char *
ngx_http_zookeeper_upstream_log_level(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);


static ngx_int_t
ngx_zookeeper_upstream_post_conf(ngx_conf_t *cf);


ngx_int_t
ngx_zookeeper_upstream_init_worker(ngx_cycle_t *cycle);


void
ngx_zookeeper_upstream_exit_worker(ngx_cycle_t *cycle);


static char *
ngx_http_zookeeper_upstream_timeout(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);


typedef struct
{
    ngx_str_t    hosts;
    ngx_int_t    timeout;
    ZooLogLevel  log_level;
} ngx_http_zookeeper_upstream_main_conf_t;


typedef struct
{
    ngx_str_t path;
    ngx_str_t lock;
    ngx_str_t file;
} ngx_http_zookeeper_upstream_srv_conf_t;


static char *
ngx_create_upsync_file(ngx_conf_t *cf, void *post, void *data);
static ngx_conf_post_t  ngx_upsync_file_post = {
    ngx_create_upsync_file
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
      ngx_http_zookeeper_upstream_timeout,
      0,
      0,
      NULL },

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

    ngx_null_command
};


static ngx_http_module_t ngx_zookeeper_upstream_ctx = {
    NULL,                                         /* preconfiguration */
    ngx_zookeeper_upstream_post_conf,             /* postconfiguration */
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
    ngx_zookeeper_upstream_init_worker,       /* init process */
    NULL,                                     /* init thread */
    NULL,                                     /* exit thread */
    ngx_zookeeper_upstream_exit_worker,       /* exit process */
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
    zmcf->timeout = 10000;

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

    return zscf;
}


static char *
ngx_http_zookeeper_upstream_log_level(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf)
{
    ngx_http_zookeeper_upstream_main_conf_t *zmcf = conf;
    ngx_str_t *values = cf->args->elts;

    if (ngx_strncasecmp((u_char*) "error", values[1].data, 5) == 0)
        zmcf->log_level = ZOO_LOG_LEVEL_ERROR;
    else if (ngx_strncasecmp((u_char*) "warn", values[1].data, 4) == 0)
        zmcf->log_level = ZOO_LOG_LEVEL_WARN;
    else if (ngx_strncasecmp((u_char*) "info", values[1].data, 4) == 0)
        zmcf->log_level = ZOO_LOG_LEVEL_INFO;
    else if (ngx_strncasecmp((u_char*) "debug", values[1].data, 5) == 0)
        zmcf->log_level = ZOO_LOG_LEVEL_DEBUG;
    else {
        ngx_log_error(NGX_LOG_ERR, cf->log, 0,
            "invalid zookeeper_log_level value (error, warn, info, debug)");
        return NGX_CONF_ERROR;
    }

    zoo_set_debug_level(zmcf->log_level);

    return NGX_CONF_OK;
}


static char *
ngx_http_zookeeper_upstream_timeout(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf)
{
    ngx_http_zookeeper_upstream_main_conf_t *zmcf = conf;
    ngx_str_t *values = cf->args->elts;

    zmcf->timeout = ngx_atoi(values[1].data, values[1].len);

    if (zmcf->timeout == (ngx_int_t) NGX_ERROR
        || zmcf->timeout < 1
        || zmcf->timeout > 60000) {
        ngx_log_error(NGX_LOG_ERR, cf->log, 0,
            "invalid value (1-60000 milliseconds)");
        return NGX_CONF_ERROR;
    }

    return NGX_CONF_OK;
}


typedef struct {
    ngx_http_upstream_srv_conf_t            *uscf;
    ngx_http_zookeeper_upstream_srv_conf_t  *zscf;
    int                                      epoch;
    ngx_flag_t                               busy;
    ngx_flag_t                               watched;
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


static void
ngx_log_message(const char *s)
{
    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, ngx_cycle->log, 0, s);
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
state_open(ngx_str_t *working_directory,
    ngx_str_t *state_file, const char *mode)
{
    u_char            path[10240];
    FILE             *f;

    if (working_directory && working_directory->len != 0)
        ngx_snprintf(path, 10240, "%V/%V\0",
                     working_directory, state_file);
    else
        ngx_snprintf(path, 10240, "%V", state_file);

    f = fopen((const char *) path, mode);
    if (f == NULL)
        ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "can't open file: %s",
                      &path);

    return f;
}


static char *
ngx_create_upsync_file(ngx_conf_t *cf, void *post, void *data)
{
    ngx_str_t        *fname = data;
    FILE             *f;
    ngx_core_conf_t  *ccf;

    static const char default_server[] = "server 0.0.0.0:1 down;";

    ccf = (ngx_core_conf_t *)
        ngx_get_conf(cf->cycle->conf_ctx, ngx_core_module);

    f = state_open(&ccf->working_directory, fname, "r");
    if (f != NULL) {
        fclose(f);
        return NGX_CONF_OK;
    }

    f = state_open(&ccf->working_directory, fname, "w+");
    if (f == NULL)
        return NGX_CONF_ERROR;

    fwrite(default_server, sizeof(default_server) - 1, 1, f);

    fclose(f);

    return NGX_CONF_OK;
}


static void
ngx_zookeeper_upstream_save(ngx_zookeeper_srv_conf_t *cfg)
{
    ngx_http_upstream_rr_peer_t   *peer;
    ngx_http_upstream_rr_peers_t  *peers, *primary;
    ngx_uint_t                     j = 0;
    u_char                         srv[10240], *c;
    FILE                          *f;
    ngx_str_t                      server;
    ngx_core_conf_t               *ccf;

    ccf = (ngx_core_conf_t *)
        ngx_get_conf(ngx_cycle->conf_ctx, ngx_core_module);

    f = state_open(&ccf->working_directory, &cfg->zscf->file, "w+");
    if (f == NULL)
        return;

    primary = cfg->uscf->peer.data;
    
    ngx_rwlock_rlock(&primary->rwlock);

    for (peers = primary;
         peers && j < 2;
         peers = peers->next, j++) {

        ngx_str_null(&server);

        for (peer = peers->peer;
             peer;
             peer = peer->next) {

            if (ngx_memn2cmp(peer->server.data, server.data,
                             peer->server.len, server.len) != 0) {
                c = ngx_snprintf(srv, 10240,
                    "server %V max_conns=%d max_fails=%d fail_timeout=%d "
                    "weight=%d",
                    &peer->server, peer->max_conns, peer->max_fails,
                    peer->fail_timeout, peer->weight);
                fwrite(srv, c - srv, 1, f);
                if (j == 1)
                    fwrite(" backup", 7, 1, f);
                fwrite(";\n", 2, 1, f);
                server = peer->server;
            }
        }
    }

    ngx_rwlock_unlock(&primary->rwlock);

    fclose(f);
}


static ngx_int_t
ngx_zookeeper_upstream_post_conf(ngx_conf_t *cf)
{
    ngx_http_upstream_main_conf_t   *umcf;
    ngx_http_upstream_srv_conf_t   **uscf;
    ngx_uint_t                       j;

    umcf = ngx_http_conf_get_module_main_conf(cf, ngx_http_upstream_module);

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
            ngx_log_error(NGX_LOG_INFO, ngx_cycle->log, 0,
                          "Zookeeper upstream: [%V] sync on", &uscf[j]->host);
        } else
            zoo.cfg[j].zscf = NULL;
    }

    return NGX_OK;
}


ngx_int_t
ngx_zookeeper_upstream_init_worker(ngx_cycle_t *cycle)
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
ngx_zookeeper_upstream_exit_worker(ngx_cycle_t *cycle)
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


static ngx_flag_t
ngx_zookeeper_exists(ngx_http_upstream_rr_peers_t *primary,
    ngx_str_t *name)
{
    ngx_http_upstream_rr_peer_t  *peer;
    ngx_http_upstream_rr_peers_t *peers;
    ngx_uint_t                    j = 0;

    ngx_rwlock_rlock(&primary->rwlock);

    for (peers = primary;
         peers && j < 2;
         peers = peers->next, j++) {

        for (peer = peers->peer;
             peer;
             peer = peer->next) {

            if (ngx_memn2cmp(peer->server.data, name->data,
                             peer->server.len, name->len) == 0) {
                ngx_rwlock_unlock(&primary->rwlock);
                return 1;
            }

            if (ngx_memn2cmp(peer->name.data, name->data,
                             peer->name.len, name->len) == 0) {
                ngx_rwlock_unlock(&primary->rwlock);
                return 1;
            }
        }
    }

    ngx_rwlock_unlock(&primary->rwlock);

    return 0;
}


static void
ngx_zookeeper_op_defaults(ngx_dynamic_upstream_op_t *op,
    ngx_str_t *upstream, ngx_str_t *server, ngx_str_t *name, int operation)
{
    ngx_memzero(op, sizeof(ngx_dynamic_upstream_op_t));

    op->op = operation;
    op->err = "unknown";

    op->status = NGX_HTTP_OK;
    op->down = 1;

    op->op_param |= NGX_DYNAMIC_UPSTEAM_OP_PARAM_RESOLVE_SYNC;

    op->server.data = server->data;
    op->server.len = server->len;

    if (name != NULL) {
        op->name.data = name->data;
        op->name.len = name->len;
    }

    op->upstream.data = upstream->data;
    op->upstream.len = upstream->len;
}

static void
ngx_zookeeper_op_defaults_locked(ngx_dynamic_upstream_op_t *op,
    ngx_str_t *upstream, ngx_str_t *server, ngx_str_t *name, int operation)
{
    ngx_zookeeper_op_defaults(op, upstream, server, name, operation);
    op->no_lock = 1;
}


static void
ngx_zookeeper_remove_obsoleted(ngx_http_upstream_srv_conf_t *uscf,
    const struct String_vector *names)
{
    ngx_http_upstream_rr_peer_t   *peer;
    ngx_http_upstream_rr_peers_t  *peers, *primary;
    ngx_uint_t                     j = 0;
    ngx_dynamic_upstream_op_t      op;
    ngx_int_t                      i;
    static ngx_str_t               noaddr = ngx_string("0.0.0.0:1");

    primary = uscf->peer.data;
    
    ngx_rwlock_wlock(&primary->rwlock);

    for (peers = primary;
         peers && j < 2;
         peers = peers->next, j++) {

        for (peer = peers->peer;
             peer;
             peer = peer->next) {

            for (i = 0; i < names->count; i++) {

                if (ngx_memn2cmp(peer->server.data, (u_char *) names->data[i],
                                 peer->server.len, strlen(names->data[i])) == 0)
                    break;

                if (ngx_memn2cmp(peer->name.data, (u_char *) names->data[i],
                                 peer->name.len, strlen(names->data[i])) == 0)
                    break;
            }

            if (i == names->count) {

again:

                ngx_zookeeper_op_defaults_locked(&op, &uscf->host,
                    &peer->server, &peer->name, NGX_DYNAMIC_UPSTEAM_OP_REMOVE);

                if (ngx_dynamic_upstream_op(ngx_cycle->log, &op, uscf)
                        == NGX_ERROR) {

                    if (op.status == NGX_HTTP_BAD_REQUEST) {

                        ngx_zookeeper_op_defaults_locked(&op, &uscf->host,
                            &noaddr, &noaddr, NGX_DYNAMIC_UPSTEAM_OP_ADD);

                        ngx_dynamic_upstream_op(ngx_cycle->log, &op,
                            uscf);

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


static void
ngx_zookeeper_sync_upstream_ready(int rc, const struct String_vector *names,
    const void *ctx)
{
    ngx_zookeeper_srv_conf_t      *cfg = (ngx_zookeeper_srv_conf_t *) ctx;
    int32_t                        j;
    ngx_dynamic_upstream_op_t      op;
    ngx_str_t                      server;

    if (rc != ZOK) {

        ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0,
                      "Zookeeper upstream: [%V] %s",
                      &cfg->uscf->host, zerror(rc));
        cfg->busy = 0;
        return;
    }

    for (j = 0; j < names->count; j++) {

        server.data = (u_char *) names->data[j];
        server.len = ngx_strlen(server.data);

        if (!ngx_zookeeper_exists(cfg->uscf->peer.data, &server)) {

            ngx_zookeeper_op_defaults(&op, &cfg->uscf->host, &server,
                NULL, NGX_DYNAMIC_UPSTEAM_OP_ADD);

            if (ngx_dynamic_upstream_op(ngx_cycle->log, &op, cfg->uscf)
                    == NGX_ERROR) {
                ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0,
                              "Zookeeper upstream: [%V] %s", &op.upstream,
                              op.err);
            }
        }
    }

    ngx_zookeeper_remove_obsoleted(cfg->uscf, names);

    if (cfg->zscf->lock.data == NULL)
        cfg->epoch = zoo.epoch;

    cfg->busy = 0;

    if (cfg->zscf->file.data)
        ngx_zookeeper_upstream_save(cfg);
}


static ngx_int_t
ngx_zookeeper_sync_update(ngx_zookeeper_srv_conf_t *cfg, ngx_flag_t force);


static void
ngx_zookeeper_sync_watch(zhandle_t *zh, int type,
    int state, const char *path, void *ctx)
{
    ngx_zookeeper_srv_conf_t  *cfg = ctx;

    if (type == ZOO_CHILD_EVENT) {

        ngx_log_error(NGX_LOG_INFO, ngx_cycle->log, 0,
                      "Zookeeper upstream: [%V] changed", &cfg->uscf->host);
        ngx_zookeeper_sync_update(cfg, 1);
    }
}


static void
ngx_zookeeper_sync_lock(int rc, const char *value, const void *ctx)
{
    const ngx_zookeeper_srv_conf_t  *cfg = ctx;
    
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


static ngx_int_t
ngx_zookeeper_sync_upstream(ngx_zookeeper_srv_conf_t *cfg)
{
    int rc;

    if (cfg->zscf->lock.data == NULL)
        rc = zoo_awget_children(zoo.handle, (const char *) cfg->zscf->path.data,
            ngx_zookeeper_sync_watch, cfg,
            ngx_zookeeper_sync_upstream_ready, cfg);
    else
        rc = zoo_aget_children(zoo.handle, (const char *) cfg->zscf->path.data,
            0, ngx_zookeeper_sync_upstream_ready, cfg);

    if (rc != ZOK) {

        ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0,
                      "Zookeeper upstream: [%V] %s",
                      &cfg->uscf->host, zerror(rc));
        cfg->busy = 0;
        return NGX_ERROR;
    }

    if (cfg->zscf->lock.data == NULL) {

        cfg->watched = 1;
        return NGX_OK;
    }

    rc = zoo_acreate(zoo.handle, (const char *) cfg->zscf->lock.data,
        "", 0, &ZOO_OPEN_ACL_UNSAFE, 0, ngx_zookeeper_sync_lock, cfg);
    if (rc == ZOK)
        return NGX_OK;

    ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0,
                  "Zookeeper upstream: [%V] failed to "
                  "register lock, %s",  &cfg->uscf->host, zerror(rc));

    return NGX_ERROR;
}


static void
ngx_zookeeper_sync_upstream_unwatch(int rc, const void *ctx)
{
    const ngx_zookeeper_srv_conf_t  *cfg = ctx;

    if (rc == ZOK) {

        ngx_log_error(NGX_LOG_INFO, ngx_cycle->log, 0,
                      "Zookeeper upstream: [%V] unwatch", &cfg->uscf->host);
        return;
    }

    ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0,
                  "Zookeeper upstream: [%V] unwatch fails, %s",
                  &cfg->uscf->host, zerror(rc));
}


static void
ngx_zookeeper_sync_upstream_exists(int rc, const struct Stat *stat,
    const void *ctx)
{
    ngx_zookeeper_srv_conf_t  *cfg = (ngx_zookeeper_srv_conf_t *) ctx;

    if (rc == ZOK) {

        if (cfg->watched)
            zoo_aremove_watchers(zoo.handle,
                (const char *) cfg->zscf->path.data, ZWATCHERTYPE_CHILDREN,
                ngx_zookeeper_sync_watch, NULL, 0,
                (void_completion_t *) ngx_zookeeper_sync_upstream_unwatch,
                NULL);

        cfg->epoch = 0;
        cfg->busy = 0;
        cfg->watched = 0;

        return;
    }

    ngx_zookeeper_sync_upstream(cfg);
}


static ngx_int_t
ngx_zookeeper_sync_update(ngx_zookeeper_srv_conf_t *cfg, ngx_flag_t force)
{
    int rc;

    if (cfg->zscf == NULL)
        return NGX_OK;

    if (cfg->epoch == zoo.epoch && !force)
        return NGX_OK;

    if (cfg->busy)
        return NGX_OK;

    cfg->busy = 1;

    if (cfg->zscf->lock.data == NULL)
        return ngx_zookeeper_sync_upstream(cfg);

    rc = zoo_aexists(zoo.handle, (const char *) cfg->zscf->lock.data,
        0, ngx_zookeeper_sync_upstream_exists, cfg);

    if (rc != ZOK) {

        ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0,
                      "Zookeeper upstream: [%V] %s",
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
            ngx_zookeeper_sync_update(&zoo.cfg[j], 0);

    return NGX_OK;
}
