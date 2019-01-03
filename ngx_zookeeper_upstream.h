#ifndef _ngx_zookeeper_upstream_h_
#define _ngx_zookeeper_upstream_h_


#include <ngx_config.h>


ngx_flag_t ngx_zookeeper_upstream_connected();
int ngx_zookeeper_upstream_epoch();
void * ngx_zookeeper_upstream_handle();

#endif
