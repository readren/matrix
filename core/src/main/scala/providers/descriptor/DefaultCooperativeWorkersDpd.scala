package readren.matrix
package providers.descriptor

import core.Matrix
import core.Matrix.{DoerProviderDescriptor, DoerProvidersManager}
import providers.{CooperativeWorkersDp, DoerProvider}

import readren.common.CompileTime.getTypeName


object DefaultCooperativeWorkersDpd extends DoerProviderDescriptor[CooperativeWorkersDp.DoerFacade](getTypeName[DefaultCooperativeWorkersDpd.type]) {
	override def build(owner: DoerProvidersManager): DoerProvider[CooperativeWorkersDp.DoerFacade] = new CooperativeWorkersDp(false)
}
