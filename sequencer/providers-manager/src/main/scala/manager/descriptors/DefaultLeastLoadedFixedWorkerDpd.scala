package readren.sequencer
package manager
package descriptors

import providers.LeastLoadedFixedWorkerDp

import readren.common.CompileTime.getTypeName

object DefaultLeastLoadedFixedWorkerDpd extends DoerProviderDescriptor[Doer](getTypeName[DefaultLeastLoadedFixedWorkerDpd.type]) {
	override type Tag = Null
	override type DP = LeastLoadedFixedWorkerDp.Impl

	override def build(owner: DoerProvidersManager): DP = new LeastLoadedFixedWorkerDp.Impl()
}
