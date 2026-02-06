"""
Forward A* Engine for Multi-Provider Zone Graph
================================================
Finds optimal multi-provider paths using standard unidirectional A* search.
"""

import heapq
import math
from typing import Dict, List, Tuple
from data_model import Postcode, Shipment, MultiHopPath, GraphIndex

class FreightAStarEngine:
    """Multi-provider zone graph pathfinding with Forward A*"""

    def __init__(self, graph_index: GraphIndex, postcodes_dict: Dict[str, Postcode]):
        self.index = graph_index
        self.postcodes = postcodes_dict
        
        # simplified state coordinates
        self.state_coords = {
            'NSW': (150.9, -33.9), 'VIC': (145.1, -37.8),
            'QLD': (153.0, -27.5), 'WA': (115.9, -31.9),
            'SA': (139.2, -34.4),  'TAS': (147.1, -42.9),
            'ACT': (149.2, -35.3), 'NT': (130.8, -12.5),
        }
    
    def find_mltihop_path(self, shipment: Shipment, maxCost: float = float('inf'), maxETD: float = float('inf'), maxHops: int = 5, topK: int = 10) -> List[MultiHopPath]:
        if shipment.originPC not in self.postcodes:
            raise ValueError(f"Origin postcode: {shipment.originPC} not found")
        if shipment.destPC not in self.postcodes:
            raise ValueError(f"Destination postcode: {shipment.destPC} not found")

        # Priority queue: (f_score, tie_breaker, current_node)
        # Node: (node_type, node_id, provider_id)
        start_node = ('pc', shipment.originPC, None)
        
        counter = 0 
        open_set = []
        
        h_start = self._heuristic(shipment.originPC, shipment.destPC)
        heapq.heappush(open_set, (h_start, counter, start_node))
        
        came_from = {}
        g_score = {start_node: 0.0}
        edge_data = {}

        found_paths = []

        while open_set:
            current_f, _, current = heapq.heappop(open_set)
            current_type, current_id, _ = current

            # GOAL REACHED
            if current_type == 'pc' and current_id == shipment.destPC:
                final_path = self._reconstruct_path(current, came_from, edge_data, shipment)
                found_paths.append(final_path)
                break 

            if g_score[current] > maxCost:
                continue

            # EXPAND FORWARD ONLY
            neighbors = self._get_forward_neighbors(current, shipment)

            for neighbor, cost, etd, info in neighbors:
                tentative_g = g_score[current] + cost
                
                if tentative_g > maxCost: continue
                
                if tentative_g < g_score.get(neighbor, float('inf')):
                    came_from[neighbor] = current
                    g_score[neighbor] = tentative_g
                    
                    h_val = self._heuristic_node(neighbor, shipment.destPC)
                    f_score = tentative_g + h_val
                    
                    edge_data[neighbor] = {'cost': cost, 'etd': etd, 'info': info}
                    
                    counter += 1
                    heapq.heappush(open_set, (f_score, counter, neighbor))

        if not found_paths:
            return [self.create_default_path(shipment)]
            
        return found_paths

    def _get_forward_neighbors(self, node: Tuple, shipment: Shipment) -> List[Tuple]:
        node_type, node_id, provider_id = node
        neighbors = []

        # 1. Postcode -> Enter Zone
        if node_type == 'pc':
            zones = self.index.get_ZonesForPostcode(node_id)
            for p_id, zone_code in zones:
                neighbor = ('pz', zone_code, p_id)
                neighbors.append((neighbor, 0.0, 0.0, {'type': 'entry', 'provider': p_id}))

        # 2. Zone -> Next Zone OR Exit
        elif node_type == 'pz':
            # Transit
            routes = self.index.get_OutgoingRoutes(provider_id, node_id)
            for route in routes:
                neighbor = ('pz', route.toZone, provider_id)
                cost = route.calculateCost(shipment.weightKG)
                neighbors.append((neighbor, cost, route.deliveryHrs, {'type': 'transit', 'route': route}))
            
            # Exit
            pcs_in_zone = self.index.get_PostcodesForZone(provider_id, node_id)
            for pc in pcs_in_zone:
                neighbor = ('pc', pc, None)
                neighbors.append((neighbor, 0.0, 0.0, {'type': 'exit', 'provider': provider_id}))

        return neighbors

    def _heuristic(self, pcA: str, pcB: str) -> float:
        objA, objB = self.postcodes.get(pcA), self.postcodes.get(pcB)
        if not objA or not objB: return 0.0
        
        coordA = self.state_coords.get(objA.state, (0,0))
        coordB = self.state_coords.get(objB.state, (0,0))
        dist = math.sqrt((coordA[0]-coordB[0])**2 + (coordA[1]-coordB[1])**2)
        return dist * 0.01

    def _heuristic_node(self, node: Tuple, goalPC: str) -> float:
        if node[0] == 'pc': return self._heuristic(node[1], goalPC)
        pcs = self.index.get_PostcodesForZone(node[2], node[1])
        return self._heuristic(pcs[0], goalPC) if pcs else 0.0

    def _reconstruct_path(self, current: Tuple, came_from: Dict, edge_data: Dict, shipment: Shipment) -> MultiHopPath:
        nodes = []
        segments = []
        total_cost = 0.0
        total_etd = 0.0
        providers = set()

        while current in came_from:
            prev = came_from[current]
            data = edge_data[current]
            
            total_cost += data['cost']
            total_etd += data['etd']

            if data['info']['type'] == 'transit':
                route = data['info']['route']
                providers.add(route.providerId)
                segments.append({
                    'providerId': route.providerId,
                    'fromZone': route.fromZone,
                    'toZone': route.toZone,
                    'cost': data['cost'],
                    'etd': data['etd']
                })
            
            nodes.append(current)
            current = prev
        
        nodes.append(current)
        nodes.reverse()
        segments.reverse()

        return MultiHopPath(
            shipmentId=shipment.id,
            totalCost=total_cost,
            totalETD=total_etd,
            nodes=nodes,
            segments=segments,
            providersInvolved=list(providers),
            numHops=len(providers)
        )

    def create_default_path(self, shipment: Shipment) -> MultiHopPath:
        return MultiHopPath(
            shipmentId=shipment.id, 
            totalCost=float('inf'), 
            totalETD=float('inf'), 
            nodes=[], 
            numHops=0
        )

class RouteOptimizer:
    def __init__(self, engine: FreightAStarEngine):
        self.engine = engine

    def unoptimized(self, shipment: Shipment) -> MultiHopPath:
        return self.engine.create_default_path(shipment)

    def optimized_for_cost(self, shipment: Shipment, maxETD: float = float('inf')) -> List[MultiHopPath]:
        return self.engine.find_mltihop_path(shipment, maxETD=maxETD)
    
    def optimized_for_time(self, shipment: Shipment, maxCost: float = float('inf')) -> List[MultiHopPath]:
        return self.engine.find_mltihop_path(shipment, maxCost=maxCost)
    
    def optimize_multi_criteria(self, shipment: Shipment) -> List[MultiHopPath]:
        return self.engine.find_mltihop_path(shipment)