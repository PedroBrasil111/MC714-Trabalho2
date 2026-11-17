"""Estado local do nó: papel, líder reconhecido, epoch e membros da replicação."""


class NodeState:
    def __init__(self, node_id: int, peers: dict[int, str], epoch: int):
        self.node_id = node_id
        self.role = "UNASSIGNED"
        self.leader_id = None
        self.epoch = epoch
        self.members = sorted(peers)
        self.synchronized = False

    def begin_election(self, epoch: int) -> None:
        self.epoch = epoch
        self.leader_id = None
        self.role = "ELECTING"
        self.synchronized = False

    def recognize_leader(self, leader_id: int, epoch: int) -> None:
        # Anúncios repetidos do mesmo líder (COORDINATOR e FENCE) não desfazem a sincronização.
        if self.leader_id == leader_id and self.epoch == epoch:
            return
        self.epoch = epoch
        self.leader_id = leader_id
        # O vencedor só passa a PRIMARY depois da recuperação.
        self.role = "LEADER_ELECTED" if leader_id == self.node_id else "BACKUP"
        self.synchronized = False

    def snapshot(self) -> dict:
        return {"node_id": self.node_id, "role": self.role, "leader_id": self.leader_id, "epoch": self.epoch}
