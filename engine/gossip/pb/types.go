package pb

import (
	"reflect"
)

// GossipMessageTypeID map protobuf go reflect type to GossipMessage_Type.
var GossipMessageTypeID = map[reflect.Type]GossipMessage_Type{
	reflect.TypeOf((*Ack)(nil)):     GossipMessage_Ack,
	reflect.TypeOf((*Sync)(nil)):    GossipMessage_Sync,
	reflect.TypeOf((*Ping)(nil)):    GossipMessage_Ping,
	reflect.TypeOf((*PingReq)(nil)): GossipMessage_PingReq,
}
