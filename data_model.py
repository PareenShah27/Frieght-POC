"""
Multi-Provider Zone Graph Data Model
====================================
Supports per-provider zone graphs with postcode-level handoffs
and bidirectional A* pathfinding without global zone reconciliation.

Maps to RDBMS schema:
- fpzones: Zone definitions + postcode mappings
- fp_pricing_rules: Zone-to-zone routes with service types
- fpcosts: Cost calculations (base + per-unit)
- fpserviceetds: Delivery time estimates
- fpvehicles: Vehicle/service capacity info
- fpfreightproviders: Provider metadata
"""

from dataclasses import dataclass, field
from typing import List, Dict, Tuple, Optional
from datetime import datetime, timezone
import uuid

@dataclass
class Postcode:
    """Global postcode node (universal across all providers)"""
    code: str
    suburb: str
    state: str
    region: Optional[str] = None

    def __hash__(self):
        return hash(('pc', self.code))
    
    def __eq__(self, other):
        if not isinstance(other, Postcode):
            return False
        return self.code == other.code

@dataclass
class ProviderZone:
    """Provider-specific zone node"""
    providerId: str
    zoneCode: str
    postcodes: List[str]
    state: str
    category: str = ""

    def __hash__(self):
        return hash(('pz', self.providerId, self.zoneCode))
    
    def __eq__(self, other):
        if not isinstance(other, ProviderZone):
            return False
        return (self.providerId, self.zoneCode) == (other.providerId, other.zoneCode)

@dataclass
class ProviderZoneRoute:
    """Zone-to-zone edge within a single provider (from fp_pricing_rules)"""
    providerId: str
    fromZone: str
    toZone: str
    serviceType: str
    baseCharge: float
    perKGRate: float
    minCharge: float
    deliveryHrs: float
    maxMass: float
    maxCBM: float = 0.0
    maxPallets: int = 0
    reliabilityScore: float = 1.0
    fuelLevyPct: float = 0.0

    def calculateCost(self, weightKG: float) -> float:
        """Calculate cost based on weight"""
        if weightKG <= 0:
            return self.minCharge
        charge = max(self.baseCharge + (weightKG * self.perKGRate), self.minCharge)
        charge += charge * (self.fuelLevyPct / 100)
        return charge

@dataclass
class Shipment:
    """Booking/shipment with pickup and delivery details"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    originPC: str = ""
    originSbrb: str = ""
    originState: str = ""
    destPC: str = ""
    destSbrb: str = ""
    destState: str = ""
    weightKG: float = 0.0
    volumeCBM: float = 0.0
    pallets: int = 0
    items: Dict[str, int] = field(default_factory=dict)
    serviceType: str = "Standard"
    createdAt: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

@dataclass
class PathNode:
    """Node in search tree (for bidirectional A*)"""
    nodeType: str  # 'pc' for Postcode, 'pz' for ProviderZone
    nodeId: str  # postcode or zoneCode
    providerId: Optional[str] # None for postcodes, provider_id for zones
    pathHops: int  # Number of hops from start
    gCost: float = 0.0  # Cost from start node
    hCost: float = 0.0  # Heuristic cost to goal
    ShipmentWeight: float = 0.0

    def __hash__(self):
        return hash((self.nodeType, self.nodeId, self.providerId))
    
    def __eq__(self, other):
        if not isinstance(other, PathNode):
            return False
        return (self.nodeType, self.nodeId, self.providerId) == (other.nodeType, other.nodeId, other.providerId)

    @property
    def fCost(self) -> float:
        """Total estimated cost (A* Priority)"""
        return self.gCost + self.hCost

@dataclass
class MultiHopPath:
    """Complete path result with multiple providers and segments"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    shipmentId: str = ""
    nodes: List[Tuple[str, str, Optional[str]]] = field(default_factory=list)  # List of (nodeType, nodeId, providerId) 
    segments: List[Dict] = field(default_factory=list)  # List of segment details -> Each segment: {provider_id, from_zone, to_zone, cost, etd, ...}

    totalCost: float = 0.0
    totalETD: float = 0.0  # in hours
    providersInvolved: List[str] = field(default_factory=list)
    numHops: int = 0
    reliabilityScore: float = 1.0
    totScore: float = 0.0

    rank: int = 0  # 1=best cost, 2=best time, 3=best overall (TOPSIS)

    def __post_init__(self):
        """Deduplicate providers"""
        self.providersInvolved = list(set(self.providersInvolved))
    
    def asDict(self) -> Dict:
        """Serialize path to dictionary"""
        return {
            "id": self.id,
            "shipmentId": self.shipmentId,
            "pathNodes": self.nodes,
            "segments": self.segments,
            "totalCost": round(self.totalCost, 2),
            "totalETD": round(self.totalETD, 1),
            "providers": self.providersInvolved,
            "numHops": self.numHops,
            "reliabilityScore": round(self.reliabilityScore, 2),
            "rank": self.rank
        }

@dataclass
class SearchState:
    """Bidirectional search metadata"""
    forwardFrontier: set[PathNode] = field(default_factory=set)
    backwardFrontier: set[PathNode] = field(default_factory=set)
    forwardVisited: dict[PathNode, float] = field(default_factory=dict)
    backwardVisited: dict[PathNode, float] = field(default_factory=dict)
    meetingPoints: List[Tuple[PathNode, PathNode]] = field(default_factory=list) 

class GraphIndex:
    """In-memory graph index for fast lookups"""
    
    def __init__(self, postcodes: List[Postcode], providerZones: Dict[str, List[ProviderZone]], zoneRoutes: Dict[str, List[ProviderZoneRoute]]):
        self.postcodes = {pc.code: pc for pc in postcodes}
        self.providerZones = providerZones
        self.zoneRoutes = zoneRoutes

        self._zoneAdj = self._buildZoneAdjacency()
        self._revZoneAdj = self._buildReverseZoneAdjacency()
        self._pcToZones = self._buildPCtoZoneMap()
        self._zoneToPCs = self._buildZoneToPCMap()

    def _buildZoneAdjacency(self) -> Dict[Tuple[str, str], List[ProviderZoneRoute]]:
        """O(routes) precomputation: (provider_id, from_zone) → [routes]"""
        adj = {}
        for route_list in self.zoneRoutes.values():
            for route in route_list:
                key = (route.providerId, route.fromZone)
                if key not in adj:
                    adj[key] = []
                adj[key].append(route)
        return adj

    def _buildReverseZoneAdjacency(self) -> Dict[Tuple[str, str], List[ProviderZoneRoute]]:
        """O(routes) precomputation: (provider_id, to_zone) → [incoming_routes]"""
        adj = {}
        for route_list in self.zoneRoutes.values():
            for route in route_list:
                key = (route.providerId, route.toZone)
                if key not in adj:
                    adj[key] = []
                adj[key].append(route)
        return adj

    def _buildPCtoZoneMap(self) -> Dict[str, List[Tuple[str, str]]]:
        """O(zones * postcodes) precomputation: postcode → [(provider_id, zone_code)]"""
        pcToZones = {}
        for providerId, zones in self.providerZones.items():
            for zone in zones:
                for pc in zone.postcodes:
                    if pc not in pcToZones:
                        pcToZones[pc] = []
                    pcToZones[pc].append((providerId, zone.zoneCode))
        return pcToZones

    def _buildZoneToPCMap(self) -> Dict[Tuple[str, str], List[str]]:
        """O(zones * postcodes) precomputation: (provider_id, zone_code) → [postcodes]"""
        zoneToPCs = {}
        for providerId, zones in self.providerZones.items():
            for zone in zones:
                key = (providerId, zone.zoneCode)
                zoneToPCs[key] = zone.postcodes
        return zoneToPCs
    
    def get_OutgoingRoutes(self, providerId: str, fromZone: str) -> List[ProviderZoneRoute]:
        """O(1): Get zone-to-zone routes from a zone"""
        return self._zoneAdj.get((providerId, fromZone), [])
    
    def get_IncomingRoutes(self, providerId: str, toZone: str) -> List[ProviderZoneRoute]:
        """O(1): Get routes ARRIVING at a zone"""
        return self._revZoneAdj.get((providerId, toZone), [])

    def get_ZonesForPostcode(self, postcode: str) -> List[Tuple[str, str]]:
        """O(1): Get (provider_id, zone_code) for a postcode"""
        return self._pcToZones.get(postcode, [])

    def get_PostcodesForZone(self, providerId: str, zoneCode: str) -> List[str]:
        """O(1): Get postcodes in a zone"""
        return self._zoneToPCs.get((providerId, zoneCode), [])
    
    def get_Providers(self) -> List[str]:
        """O(1): Get list of all providers in the graph"""
        return list(self.providerZones.keys())
    
    def get_AllZones(self, providerId: str) -> List[ProviderZone]:
        """O(1): Get all zones across all providers"""
        zones = []
        for pz_list in self.providerZones.values():
            zones.extend(pz_list)
        return zones