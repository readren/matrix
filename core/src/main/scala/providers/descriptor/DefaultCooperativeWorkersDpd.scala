package readren.matrix
package providers.descriptor

import common.CompileTime
import core.Matrix
import core.Matrix.{DoerProviderDescriptor, DoerProvidersManager}
import providers.assistant.{CooperativeWorkersDap, DoerProvider}


object DefaultCooperativeWorkersDpd extends DoerProviderDescriptor[CooperativeWorkersDap.DoerFacade](CompileTime.getTypeName[DefaultCooperativeWorkersDpd.type]) {
	override def build(owner: DoerProvidersManager): DoerProvider[CooperativeWorkersDap.DoerFacade] = new CooperativeWorkersDap(false)
}
