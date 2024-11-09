package readren.matrix

import readren.taskflow.Doer


class MatrixAdmin(val id: Int, val assistant: Doer.Assistant, val matrix: Matrix) extends Doer(assistant)
