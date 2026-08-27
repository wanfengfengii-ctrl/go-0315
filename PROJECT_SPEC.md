# 数字档案副本完整性核验与受控修复服务

## 项目目标

面向长期保存机构构建数字档案完整性后端。服务冻结保存批次及校验策略，从多个独立存储节点采集对象清单和分块摘要，以法定副本数判定缺失、截断、静默位翻转和版本分叉；异常对象进入隔离代次，系统选择可信源、生成可重试修复任务，并在重读校验、稳定观察和双人复核后解除隔离。SQLite保存批次、证据、租约、修复链和终局，启动扫描恢复未完成工作。生产实现要求20至30个Go文件、至少4个有实际职责的包，约2500有效生产行且保持在2000至3000行；测试不得计入行数目标，公开测试至少4个文件、12个独立用例。

## 端到端业务流程

1. 创建保存批次，写入对象标识、预期长度、分块规格、摘要算法、节点名册、法定副本数、扫描日程、稳定窗口和复核人员；冻结后生成不可变策略摘要。
2. 扫描节点清单并提交带操作号的对象证据。服务规范化分块序号与摘要，在同一事务中验证扫描租约、写入只追加证据并按不同节点计票；幂等重放返回原结果。
3. 达到法定副本数后形成唯一完整性裁定：识别副本缺失、长度不符、分块损坏、摘要不符或内容分叉，并以可信摘要、对象依赖和共同存储故障域计算确定的隔离集合。
4. 为隔离代次选择满足法定证据且不在故障域内的可信源，按目标节点生成修复任务。外部复制发生超时、拒绝或响应异常时，持久化固定退避序号并确定性重试。
5. 修复完成后重新读取目标分块；只有完整摘要匹配、各节点证据覆盖达标且稳定窗口内无新分叉，才允许两名不同合格人员复核。
6. 终局请求原子竞争解除隔离、继续隔离或废止；解除隔离胜出时生成该代次唯一恢复凭据，重启不得重复修复已确认分块或改变既有裁定。

## 核心组件与职责

1. 批次与保存策略目录：规范化对象标识，校验分块布局、节点故障域和摘要策略，事务冻结不可变批次。
2. 扫描证据与完整性裁定器：管理扫描纪元、只追加副本证据、法定副本计票、内容分叉裁定及隔离集合闭包。
3. 租约与修复编排器：管理扫描、源读取、目标写入和终局四类限时租约，创建分块修复链与确定重试任务。
4. 稳定验证与恢复仲裁器：执行修复后重读、覆盖率和稳定窗口计算、双人复核、单终局竞争及恢复凭据签发。
5. 持久化与启动恢复层：使用SQLite事务、唯一约束和版本比较保存领域状态，回收过期租约并恢复待执行任务。
6. 版本化HTTP接口：提供严格JSON校验、幂等键、稳定错误码、确定排序及健康检查，所有状态变更经领域状态机执行。

## 领域规则与不变量

1. 对象标识采用UTF-8规范形式且批次内唯一；对象长度必须非负，分块大小为1至64MiB，分块区间连续、无重叠并精确覆盖对象长度，空对象仅有整体摘要。
2. 摘要仅接受冻结策略声明的算法和固定字节长度；分块按零基连续序号排序。对象根摘要由域分隔符、对象长度、分块序号及分块摘要按固定二进制编码计算。
3. 证据由批次、对象、纪元、节点、分块唯一定位且只追加；相同操作号仅在规范化内容完全一致时幂等，内容变化返回IDEMPOTENCY_CONFLICT。
4. 法定副本数只统计不同且处于不同冻结身份的节点。多个候选摘要同时达标时，按达到门槛的逻辑时刻、摘要字节序选择唯一裁定；旧纪元证据不得回退当前裁定。
5. 隔离集合从异常对象出发，沿对象依赖边、共享内容根和共同故障域边做确定广度遍历；成员按对象标识排序并去重。新异常生成递增代次，旧代次结果不能改变当前状态。
6. 可信源必须匹配当前法定摘要、具备完整分块证据且不属于被裁定的故障域；目标不得与源相同。修复回执必须匹配任务、代次、分块范围和预期摘要。
7. 租约使用数据库逻辑时钟半开区间[start,expires)，now等于expires即失效；同一资源最多一个持有者，续租必须匹配持有者、租约号和版本。
8. 覆盖率使用floor(有效节点数×10000/冻结节点数)；分母为零及乘法溢出均拒绝计算。稳定窗口内出现摘要变化、节点证据撤回或新分叉即从该逻辑时刻重新计时。

## 数据模型与持久化

1. PreservationBatch：batch_id、generation、status、policy_digest、current_epoch、terminal_version；FrozenPolicy：chunk_size、hash_algorithm、replica_quorum、coverage_bps、stable_ticks、schedule。
2. ArchiveObject：object_id、canonical_key、expected_length、expected_root；ObjectDependency：from_object、to_object、reason；StorageNode：node_id、failure_domain、enabled。
3. ScanEpoch：batch_id、epoch_no、opened_tick、closed_tick；ReplicaEvidence：object_id、epoch_no、node_id、chunk_no、length、digest、operation_id、observed_tick。
4. IntegrityVerdict：object_id、epoch_no、winning_root、verdict_kind、threshold_tick；IsolationMember：generation、object_id、reason、parent_object。
5. ResourceLease：resource_type、resource_key、lease_id、holder、start_tick、expires_tick、version；RepairTask：generation、object_id、chunk_no、source_node、target_node、expected_digest、state、attempt_no；PendingAttempt保存next_tick和失败类别。
6. VerificationSample：generation、object_id、node_id、root_digest、sample_tick；ReviewDecision、TerminalDecision和RecoveryCredential分别保存复核、唯一终局及唯一恢复凭据。

## 公开接口

1. POST /v1/batches、PUT /v1/batches/{id}/catalog、POST /v1/batches/{id}/freeze：创建、填充并冻结保存批次。
2. POST /v1/batches/{id}/epochs、POST /v1/batches/{id}/epochs/{epoch}/evidence、POST /v1/batches/{id}/epochs/{epoch}/close：开启扫描、提交证据并形成裁定。
3. POST /v1/leases/acquire、POST /v1/leases/{id}/renew、POST /v1/leases/{id}/release：管理四类资源租约；请求显式携带单调logical_tick。
4. POST /v1/batches/{id}/generations/{gen}/repairs、POST /v1/repairs/{id}/dispatch、POST /v1/repairs/{id}/receipt：编排修复、记录外部结果并核验回执。
5. POST /v1/batches/{id}/generations/{gen}/samples、POST /v1/batches/{id}/generations/{gen}/reviews、POST /v1/batches/{id}/terminal：提交验证样本、复核和终局竞争。
6. GET /v1/batches/{id}及子资源返回确定排序的重建状态；写接口接受Idempotency-Key，错误格式为{code,message,details}。

## 失败边界

1. 冻结批次、证据写入与计票、隔离闭包与修复建链、回执核验、复核和终局签发分别使用单一数据库事务，失败不得留下部分有效状态。
2. 存储节点位于事务外；服务先持久化调用意图，再调用适配器，最后以版本比较记录结果。超时、拒绝、断连、格式错误和摘要不符只能进入失败或待重试状态。
3. 进程重启后恢复冻结策略、证据、修复链和终局；启动扫描使过期租约失效，将无结果且未耗尽次数的调用恢复为待执行，并跳过已有有效回执的分块。
4. 并发关闭纪元、创建隔离代次或提交终局依赖唯一约束和乐观版本；失败者返回QUORUM_CONFLICT、STALE_GENERATION或TERMINAL_CONFLICT。
5. HTTP层限制请求体、对象数、分块数和整数范围；畸形JSON、未知字段及规范化错误在事务前拒绝，数据库错误不得映射为成功。

## 验收标准

1. 冻结后，批次对象、摘要策略、节点、故障域、法定副本数、日程及阈值均不可修改；重启后策略摘要和内容一致。
2. 对象布局、根摘要和证据排序采用公开确定算法；空洞、重叠、越界分块及错误摘要长度被稳定拒绝。
3. 并发证据提交与计票原子完成；幂等重试不增加证据，同号异内容、重复节点占票或过期租约整体回滚。
4. 每个对象当前纪元只有一个有效裁定；异常精确生成排序后的隔离闭包，新代次保留旧证据且不接受旧回执推进。
5. 修复仅从合格可信源写向目标；超时、拒绝、断连或摘要不符产生确定重试，重启后不重复已确认分块。
6. 覆盖率、逻辑时间和稳定窗口遵循整数及半开边界规则，任何新分叉都会重置稳定观察。
7. 只有修复后全量摘要匹配、覆盖达标、稳定窗口完成且两名不同合格人员复核后，才能签发唯一恢复凭据。
8. 生产代码达到20至30个Go文件、至少4个包及2000至3000有效行；至少4个公开测试文件包含12个以上确定性测试，并可在amd64与arm64容器中构建运行。

## 确定性测试场景

1. layout_hash_test.go：验证空对象、单分块、多分块根摘要及固定排序。
2. layout_hash_test.go：分块空洞、重叠、越界和错误摘要长度分别被拒绝。
3. evidence_quorum_test.go：三个节点并发支持两个候选根时只产生一个门槛胜者。
4. evidence_quorum_test.go：同操作号重放幂等，同号变更内容回滚且票数不增加。
5. evidence_quorum_test.go：租约在expires边界失效，两个持有者并发竞争仅一个成功。
6. isolation_repair_test.go：分块损坏沿依赖、共享内容根和故障域边生成精确隔离闭包。
7. isolation_repair_test.go：可信源位于异常故障域或证据不完整时不得创建修复任务。
8. isolation_repair_test.go：适配器依次超时、拒绝、成功，断言固定重试tick和唯一有效回执。
9. isolation_repair_test.go：修复中途重启后只继续未确认分块。
10. recovery_test.go：覆盖率验证整除、向下取整、零分母和溢出边界。
11. recovery_test.go：稳定窗口边界出现新分叉时重置计时。
12. recovery_test.go：同一人员重复复核不能满足门槛，两名不同合格人员可以。
13. recovery_test.go：并发提交三种终局请求时仅一个胜出且最多生成一个恢复凭据。

## 组件追踪关系

1. 批次与保存策略目录对应冻结、对象布局、摘要策略和节点故障域规则，由layout_hash_test.go覆盖。
2. 扫描证据与完整性裁定器对应只追加证据、幂等、法定副本数、唯一裁定和隔离闭包，由evidence_quorum_test.go与isolation_repair_test.go覆盖。
3. 租约与修复编排器对应互斥租约、可信源选择、修复链和确定重试，由evidence_quorum_test.go与isolation_repair_test.go覆盖。
4. 稳定验证与恢复仲裁器对应覆盖率、稳定窗口、双人复核、唯一终局和恢复凭据，由recovery_test.go覆盖。
5. 持久化与启动恢复层对应事务回滚、唯一约束、过期回收及重启续作，由isolation_repair_test.go和recovery_test.go的重启用例覆盖。
6. 版本化HTTP接口对应严格校验、错误码、幂等键和确定排序，四个公开测试文件均使用httptest验证。

## 独特性

项目围绕数字档案长期保存中的多副本可信性展开，将分块证据法定裁定、故障域传播形成的隔离闭包、可恢复修复链和双人解除隔离统一为一条可确定复现的完整性恢复流程。
