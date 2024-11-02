package readren.matrix


import scala.collection.mutable
import readren.taskflow.{Doer, Maybe}

import scala.compiletime.uninitialized

/** @param admin the [[MatrixAdmin]] assigned to this inbox and the reactant that owns it. All mutable members of this inbox instance should be accessed within this [[MatrixAdmin]]. */
class FifoInbox[M](override val admin: MatrixAdmin) extends InboxBackend[M] { thisFifoInbox =>

	/**
	 * The owner of this inbox. 
	 * Should be accessed only within the [[admin]] */
	private var owner: Reactant[M] | Null = uninitialized

	/** Should be accessed only within the [[admin]] */
	private val queue: mutable.ArrayDeque[M] = mutable.ArrayDeque.empty

	override def submit(message: M): Unit = {
		admin.queueForSequentialExecution {
			val wasEmpty = queue.isEmpty
			queue.append(message)
			if wasEmpty && null.ne(owner) && owner.asInstanceOf[Reactant[M]].isIdle then admin.stimulate(owner, thisFifoInbox)
		}
	}

	override def withdraw(): admin.Duty[Maybe[M]] = {
		admin.Duty.mine { () =>
			if queue.isEmpty then Maybe.empty
			else Maybe.some(queue.removeHead())
		}
	}

	override def setOwner(reactant: Reactant[M], asker: MatrixAdmin): Unit = {
		inline def work(): Unit = {
			assert(owner.admin eq admin)
			owner = reactant
			if queue.nonEmpty then admin.stimulate(owner, thisFifoInbox)
		}

		if admin eq asker then work()
		else admin.queueForSequentialExecution(work())
	}

}
