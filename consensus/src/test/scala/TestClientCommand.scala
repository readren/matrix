package readren.consensus

// Test command type
case class TestClientCommand(serial: Int, clientId: String) extends Serializable
