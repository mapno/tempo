{
  local k = import 'k.libsonnet',
  local kausal = import 'ksonnet-util/kausal.libsonnet',

  local container = k.core.v1.container,
  local containerPort = kausal.core.v1.containerPort,
  local deployment = k.apps.v1.deployment,
  local statefulset = k.apps.v1.statefulSet,
  local volume = k.core.v1.volume,
  local pvc = k.core.v1.persistentVolumeClaim,
  local volumeMount = k.core.v1.volumeMount,

  local target_name = 'block-builder',
  local tempo_config_volume = 'tempo-conf',
  local tempo_overrides_config_volume = 'overrides',

  tempo_block_builder_ports:: [containerPort.new('prom-metrics', $._config.port)],
  tempo_block_builder_args:: {
    target: target_name,
    'config.file': '/conf/tempo.yaml',
    'mem-ballast-size-mbs': $._config.ballast_size_mbs,
  },

  tempo_block_builder_container::
    container.new(target_name, $._images.tempo) +
    container.withPorts($.tempo_block_builder_ports) +
    container.withArgs($.util.mapToFlags($.tempo_block_builder_args)) +
    container.withVolumeMounts([
      volumeMount.new(tempo_config_volume, '/conf'),
      volumeMount.new(tempo_overrides_config_volume, '/overrides'),
    ]) +
    $.util.withResources($._config.block_builder.resources) +
    (if $._config.variables_expansion then container.withEnvMixin($._config.variables_expansion_env_mixin) else {}) +
    container.mixin.resources.withRequestsMixin({ 'ephemeral-storage': $._config.block_builder.ephemeral_storage_request_size }) +
    container.mixin.resources.withLimitsMixin({ 'ephemeral-storage': $._config.block_builder.ephemeral_storage_limit_size }) +
    $.util.readinessProbe +
    (if $._config.variables_expansion then container.withArgsMixin(['-config.expand-env=true']) else {}),

  tempo_block_builder_deployment:
    deployment.new(
      target_name,
      0,
      $.tempo_block_builder_container,
      {
        app: target_name,
        [$._config.gossip_member_label]: 'true',
      },
    ) +
    deployment.mixin.spec.strategy.rollingUpdate.withMaxSurge(3) +
    deployment.mixin.spec.strategy.rollingUpdate.withMaxUnavailable(1) +
    deployment.mixin.spec.template.metadata.withAnnotations({
      config_hash: std.md5(std.toString($.tempo_block_builder_configmap.data['tempo.yaml'])),
    }) +
    deployment.mixin.spec.template.spec.withVolumes([
      volume.fromConfigMap(tempo_config_volume, $.tempo_block_builder_configmap.metadata.name),
      volume.fromConfigMap(tempo_overrides_config_volume, $._config.overrides_configmap_name),
    ])
  ,

  newGeneratorStatefulSet(name, container, with_anti_affinity=true)::
    statefulset.new(
      name,
      $._config.block_builder.replicas,
      $.tempo_block_builder_container,
      self.tempo_block_builder_pvc,
      {
        app: target_name,
        [$._config.gossip_member_label]: 'true',
      },
    )
    + kausal.util.antiAffinityStatefulSet
    + statefulset.mixin.spec.withServiceName(target_name)
    + statefulset.mixin.spec.template.metadata.withAnnotations({
      config_hash: std.md5(std.toString($.tempo_block_builder_configmap.data['tempo.yaml'])),
    })
    + statefulset.mixin.spec.template.spec.withVolumes([
      volume.fromConfigMap(tempo_config_volume, $.tempo_block_builder_configmap.metadata.name),
      volume.fromConfigMap(tempo_overrides_config_volume, $._config.overrides_configmap_name),
    ]) +
    statefulset.mixin.spec.withPodManagementPolicy('Parallel') +
    $.util.podPriority('high') +
    (if with_anti_affinity then $.util.antiAffinity else {}),

  tempo_block_builder_statefulset:
    $.newGeneratorStatefulSet(target_name, self.tempo_block_builder_container)
    + statefulset.spec.template.spec.securityContext.withFsGroup(10001)  // 10001 is the UID of the tempo user
    + statefulset.mixin.spec.withReplicas($._config.block_builder.replicas),

  tempo_block_builder_service:
    kausal.util.serviceFor($.tempo_block_builder_deployment),
}
