package readren.matrix
package cluster.misc

import providers.assistant.DoerAssistantProvider

import readren.taskflow.{AbstractDoer, SchedulingExtension}

abstract class TaskSequencer extends AbstractDoer, SchedulingExtension