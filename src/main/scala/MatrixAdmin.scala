package readren.matrix

import constants.*

import readren.taskflow.Doer.ExceptionReport
import readren.taskflow.{Doer, Maybe}


class MatrixAdmin(val assistant: Doer.Assistant, val matrix: Matrix) extends Doer(assistant) { thisAdmin =>

}
