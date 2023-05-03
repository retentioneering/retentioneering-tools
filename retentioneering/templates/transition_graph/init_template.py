init_code = """
    initialize({{
    serverId: '{server_id}',
    env: '{env}',
    selectedNormalizationTypeId: '{norm_type}',
    configNodes: {nodes},
    configLinks: {links},
    nodesColsNames: {node_cols_names},
    linksWeightsNames: {links_weights_names},
    nodesThreshold: {nodes_threshold},
    linksThreshold: {links_threshold},
    shownNodesCol: '{shown_nodes_col}',
    shownLinksWeight: '{shown_links_weight}',
    selectedNodesColForThresholds: '{selected_nodes_col_for_thresholds}',
    selectedLinksWeightForThresholds: '{selected_links_weight_for_thresholds}',
    showWeights: {show_weights},
    showPercents: {show_percents},
    showNodesNames: {show_nodes_names},
    showAllEdgesForTargets: {show_all_edges_for_targets},
    showNodesWithoutLinks: {show_nodes_without_links},
    useLayoutDump: Boolean({layout_dump}),
    weightTemplate: {weight_template},
    trackingHWID: '{tracking_hardware_id}'
}})
"""
