package readren.common

/** Helper to generate toString with field names. */
extension (self: Matchable) {
	/** Stringifies with field names.
	 * @note Is very inefficient. Use it only for debugging. */
	def toStringWithFields: String = {
		self match {
			case p: Product =>
				val fields = p.productElementNames.zip(p.productIterator).map((n, v) => s"$n=$v").mkString(", ")
				s"${p.productPrefix}($fields)"
			case x =>
				x.toString
		}
	}
}




