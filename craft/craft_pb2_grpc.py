# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
import grpc

import craft_pb2 as craft__pb2


class cRaftStub(object):
  """cRaft gRPC implementation
  """

  def __init__(self, channel):
    """Constructor.

    Args:
      channel: A grpc.Channel.
    """
    self.RequestVote = channel.unary_unary(
        '/cRaft/RequestVote',
        request_serializer=craft__pb2.VoteRequest.SerializeToString,
        response_deserializer=craft__pb2.Ack.FromString,
        )
    self.AppendEntries = channel.unary_unary(
        '/cRaft/AppendEntries',
        request_serializer=craft__pb2.Entries.SerializeToString,
        response_deserializer=craft__pb2.Ack.FromString,
        )
    self.ReceivePropose = channel.unary_unary(
        '/cRaft/ReceivePropose',
        request_serializer=craft__pb2.Proposal.SerializeToString,
        response_deserializer=craft__pb2.Ack.FromString,
        )
    self.Notified = channel.unary_unary(
        '/cRaft/Notified',
        request_serializer=craft__pb2.Entry.SerializeToString,
        response_deserializer=craft__pb2.Ack.FromString,
        )


class cRaftServicer(object):
  """cRaft gRPC implementation
  """

  def RequestVote(self, request, context):
    """Takes an ID (use shared IDKey message type) and returns k nodes with
    distance closest to ID requested
    """
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def AppendEntries(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def ReceivePropose(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def Notified(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')


def add_cRaftServicer_to_server(servicer, server):
  rpc_method_handlers = {
      'RequestVote': grpc.unary_unary_rpc_method_handler(
          servicer.RequestVote,
          request_deserializer=craft__pb2.VoteRequest.FromString,
          response_serializer=craft__pb2.Ack.SerializeToString,
      ),
      'AppendEntries': grpc.unary_unary_rpc_method_handler(
          servicer.AppendEntries,
          request_deserializer=craft__pb2.Entries.FromString,
          response_serializer=craft__pb2.Ack.SerializeToString,
      ),
      'ReceivePropose': grpc.unary_unary_rpc_method_handler(
          servicer.ReceivePropose,
          request_deserializer=craft__pb2.Proposal.FromString,
          response_serializer=craft__pb2.Ack.SerializeToString,
      ),
      'Notified': grpc.unary_unary_rpc_method_handler(
          servicer.Notified,
          request_deserializer=craft__pb2.Entry.FromString,
          response_serializer=craft__pb2.Ack.SerializeToString,
      ),
  }
  generic_handler = grpc.method_handlers_generic_handler(
      'cRaft', rpc_method_handlers)
  server.add_generic_rpc_handlers((generic_handler,))
