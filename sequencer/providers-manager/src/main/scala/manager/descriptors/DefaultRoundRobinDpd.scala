package readren.sequencer
package manager
package descriptors

import manager.descriptors.DefaultAsyncSchedulingDpd.Tag
import providers.RoundRobinDp

import readren.common.CompileTime.getTypeName


object DefaultRoundRobinDpd extends DoerProviderDescriptor[Doer](getTypeName[DefaultRoundRobinDpd.type]) {
	override type Tag = Null
	override type DP = RoundRobinDp.Impl

	override def build(owner: DoerProvidersManager): DP = new RoundRobinDp.Impl()
}
