package readren.matrix
package providers.descriptor

import core.Matrix
import core.Matrix.{DoerProviderDescriptor, DoerProvidersManager}
import readren.sequencer.providers.CooperativeWorkersDp

import readren.common.CompileTime.getTypeName
import readren.sequencer.DoerProvider


object DefaultCooperativeWorkersDpd extends DoerProviderDescriptor[CooperativeWorkersDp.DoerFacade](getTypeName[DefaultCooperativeWorkersDpd.type]) {
	override def build(owner: DoerProvidersManager): DoerProvider[CooperativeWorkersDp.DoerFacade] = new CooperativeWorkersDp.Impl(false)
}
