// Copyright 2010-2013 RethinkDB, all rights reserved.
#ifndef UNITTEST_TEST_CLUSTER_GROUP_HPP_
#define UNITTEST_TEST_CLUSTER_GROUP_HPP_

#include <map>
#include <string>
#include <set>

#include "errors.hpp"
#include <boost/optional.hpp>
#include <boost/ptr_container/ptr_vector.hpp>

#include "containers/cow_ptr.hpp"
#include "containers/scoped.hpp"
#include "clustering/reactor/directory_echo.hpp"
#include "buffer_cache/alt/cache_balancer.hpp"
#include "rdb_protocol/protocol.hpp"
#include "rpc/connectivity/multiplexer.hpp"
#include "rpc/directory/read_manager.hpp"
#include "rpc/directory/write_manager.hpp"
#include "unittest/branch_history_manager.hpp"
#include "unittest/mock_store.hpp"

class blueprint_t;
class cluster_namespace_interface_t;
class io_backender_t;
class multistore_ptr_t;
class reactor_business_card_t;
class peer_id_t;
class serializer_t;

namespace unittest {

class temp_file_t;

class reactor_test_cluster_t;
class test_reactor_t;

class test_cluster_directory_t {
public:
    boost::optional<directory_echo_wrapper_t<cow_ptr_t<reactor_business_card_t> > > reactor_directory;

    RDB_DECLARE_ME_SERIALIZABLE;
};


class test_cluster_group_t {
public:
    const base_path_t base_path;
    boost::ptr_vector<temp_file_t> files;
    scoped_ptr_t<io_backender_t> io_backender;
    scoped_ptr_t<cache_balancer_t> balancer;
    boost::ptr_vector<serializer_t> serializers;
    boost::ptr_vector<mock_store_t> stores;
    boost::ptr_vector<multistore_ptr_t> svses;
    boost::ptr_vector<reactor_test_cluster_t> test_clusters;

    boost::ptr_vector<test_reactor_t> test_reactors;

    std::map<std::string, std::string> inserter_state;

    rdb_context_t ctx;

    explicit test_cluster_group_t(int n_machines);
    ~test_cluster_group_t();

    void construct_all_reactors(const blueprint_t &bp);

    peer_id_t get_peer_id(unsigned i);

    blueprint_t compile_blueprint(const std::string& bp);

    void set_all_blueprints(const blueprint_t &bp);

    static std::map<peer_id_t, cow_ptr_t<reactor_business_card_t> > extract_reactor_business_cards_no_optional(
            const change_tracking_map_t<peer_id_t, test_cluster_directory_t> &input);

    void make_namespace_interface(int i, scoped_ptr_t<cluster_namespace_interface_t> *out);

    void run_queries();

    static std::map<peer_id_t, boost::optional<cow_ptr_t<reactor_business_card_t> > > extract_reactor_business_cards(
            const change_tracking_map_t<peer_id_t, test_cluster_directory_t> &input);

    void wait_until_blueprint_is_satisfied(const blueprint_t &bp);

    void wait_until_blueprint_is_satisfied(const std::string& bp);
};

/* This is a cluster that is useful for reactor testing... but doesn't actually
 * have a reactor due to the annoyance of needing the peer ids to create a
 * correct blueprint. */
class reactor_test_cluster_t {
public:
    explicit reactor_test_cluster_t(int port);
    ~reactor_test_cluster_t();

    peer_id_t get_me();

    connectivity_cluster_t connectivity_cluster;
    message_multiplexer_t message_multiplexer;

    message_multiplexer_t::client_t heartbeat_manager_client;
    heartbeat_manager_t heartbeat_manager;
    message_multiplexer_t::client_t::run_t heartbeat_manager_client_run;

    message_multiplexer_t::client_t mailbox_manager_client;
    mailbox_manager_t mailbox_manager;
    message_multiplexer_t::client_t::run_t mailbox_manager_client_run;

    watchable_variable_t<test_cluster_directory_t> our_directory_variable;
    message_multiplexer_t::client_t directory_manager_client;
    directory_read_manager_t<test_cluster_directory_t> directory_read_manager;
    directory_write_manager_t<test_cluster_directory_t> directory_write_manager;
    message_multiplexer_t::client_t::run_t directory_manager_client_run;

    message_multiplexer_t::run_t message_multiplexer_run;

    connectivity_cluster_t::run_t connectivity_cluster_run;

    in_memory_branch_history_manager_t branch_history_manager;
};

}  // namespace unittest

#endif  // UNITTEST_TEST_CLUSTER_GROUP_HPP_
