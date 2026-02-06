"""
TerminusDB Data Loader for Freight Recommendation Engine
=========================================================

Loads sample freight network data into TerminusDB graph database (hosted on Docker).
Supports both PoC (sample data) and Production (RDBMS) modes.

Schema Design:
- Postcode: Vertices representing Australian postcodes
- ProviderZone: Vertices for provider-specific zones
- ProviderZoneRoute: Edges connecting zones with cost/time metadata
- Shipment: Query vertices for origin/destination

Integration:
- Automatic graph creation + schema validation
- Bulk insert for sample data (PoC mode)
- SQL Server integration for production data (RDBMS mode)
"""

import os
import json
import logging
from typing import Union, Any, Dict, List, Tuple
from datetime import datetime
from dataclasses import asdict

try:
    from terminusdb_client import WOQLClient, WOQLQuery, GraphType
except ImportError:
    raise ImportError("TerminusDB client library is not installed. Install it using: pip install terminusdb-client")

from data_model import Postcode, ProviderZone, ProviderZoneRoute, GraphIndex

logging.basicConfig(level=logging.INFO)
logger =logging.getLogger(__name__)

class TerminusDBLoader:
    """
    Manages TerminusDB graph operations for freight network data.

    Features:
    - Automatic schema creation
    - PoC data generation and bulk loading
    - RDBMS integration for production data
    - Graph validation + consistency checks
    - Query builder for complex searches
    """

    def __init__(self, server_url: str = "http://localhost:6363",
        db_name: str = "freight_poc",
        username: str = "admin",
        password: str = "root",
        use_rdbms: bool = False):
        """
        Initialize TerminusDB client.

        Args:
            server_url: TerminusDB server endpoint (default: localhost:6363)
            db_name: Database/graph name (default: freight_poc)
            username: Authentication username
            password: Authentication password
            use_rdbms: If True, load from SQL Server; if False, use PoC data
        """

        self.serverURL = server_url
        self.dbName = db_name
        self.userRDBMS = use_rdbms

        # Initialize TerminusDB client
        if WOQLClient is None:
            raise ImportError("TerminusDB client library is not installed.")
        
        self.client = WOQLClient(server_url=self.serverURL)
        try:
            self.client.connect(user=username, password=password)
            logger.info(f"Connected to TerminusDB at {self.serverURL} as {username}")
        except Exception as e:
            logger.warning(f"Could not connect to TerminusDB: {e}")
    
    def create_database(self, force_recreate: bool = False) -> bool:
        """
        Create or validate TerminusDB graph database.

        Args:
            force_recreate: If True, delete existing DB and recreate

        Returns:
            True if database created/validated successfully
        """
        try:
            # Check if database exists
            databases = self.client.get_databases()
            db_exists = any(db["name"] == self.dbName for db in databases)

            if db_exists:
                if force_recreate:
                    logger.info(f"Database {self.dbName} exists. Deleting for recreation.")
                    self.client.delete_database(self.dbName)
                    db_exists = False
                else:
                    logger.info(f"Database {self.dbName} already exists. Validating schema.")
                    return True
            
            # Create new database
            if not db_exists:
                logger.info(f"Creating database {self.dbName}.")
                self.client.create_database(self.dbName, label="Freight Recommendation Engine PoC", description="Graph DB for freight network data")
                logger.info(f"Database {self.dbName} created successfully.")
                return True
        except Exception as e:
            logger.error(f"Error creating/validating database {self.dbName}: {e}")
            raise 
        return False

    def createSchema(self) -> bool:
        """
        Create TerminusDB schema for freight network.

        Schema structure:
        - Postcode: Australian postcode (vertex)
        - ProviderZone: Provider-specific delivery zone (vertex)
        - ProviderZoneRoute: Delivery route between zones (edge)
        - ZoneProviderAssociation: Maps postcodes to zones

        Returns:
            True if schema created successfully
        """
        try:
            logger.info("Creating graph schema...")
            
            # Standard List of Classes format (No 'Context' wrapper, no 'doc:' prefixes)
            schema_objects = [
                {
                    "@type": "Class",
                    "@id": "Shipment",
                    "origin_pc": "xsd:string",
                    "dest_pc": "xsd:string",
                    "weight_kg": "xsd:decimal",
                    "volume_cbm": "xsd:decimal"
                },
                {
                    "@type": "Class",
                    "@id": "Postcode",
                    "code": "xsd:string",
                    "suburb": "xsd:string",
                    "state": "xsd:string"
                },
                {
                    "@type": "Class",
                    "@id": "ProviderZone",
                    "provider_id": "xsd:string",
                    "zone_name": "xsd:string",
                    "state": "xsd:string",
                    "category": "xsd:string",
                    "postcodes": { "@type": "Set", "@class": "xsd:string" } 
                },
                {
                    "@type": "Class",
                    "@id": "ProviderZoneRoute",
                    "provider_id": "xsd:string",
                    "from_zone": "xsd:string",
                    "to_zone": "xsd:string",
                    "service_type": "xsd:string",
                    "base_cost": "xsd:decimal",
                    "cost_per_kg": "xsd:decimal",
                    "min_charge": "xsd:decimal",
                    "etd_hours": "xsd:decimal",
                    "max_weight_kg": "xsd:decimal",
                    "max_cbm": "xsd:decimal",
                    "max_pallets": "xsd:integer",
                    "reliability_score": "xsd:decimal",
                    "fuel_levy_pct": "xsd:decimal"
                }
            ]

            self.client.insert_document(schema_objects, graph_type=GraphType.SCHEMA)
            logger.info("Graph schema created successfully.")
            return True
        except Exception as e:
            logger.warning(f"Schema Creation Error: {e}")
            return False
    
    def load_sample_data(self) -> Tuple[Dict, List[Postcode], List[ProviderZone], List[ProviderZoneRoute], GraphIndex]:
        """
        Generate and load PoC sample data into TerminusDB.

        Data includes:
        - 10 Australian postcodes
        - 5 freight providers with 4 zones each
        - 40 zone-to-zone routes with pricing/ETD

        Returns:
            Tuple of (graph_index_dict, postcodes, zones, routes)
        """
        logger.info("Generating PoC sample data...")

        # Sample postcodes
        postcodes_data = [
            {"code": "2000", "suburb": "Sydney CBD", "state": "NSW"},
            {"code": "2010", "suburb": "Pyrmont", "state": "NSW"},
            {"code": "2050", "suburb": "Neutral Bay", "state": "NSW"},
            {"code": "2100", "suburb": "Chatswood", "state": "NSW"},
            {"code": "3000", "suburb": "Melbourne CBD", "state": "VIC"},
            {"code": "3010", "suburb": "Docklands", "state": "VIC"},
            {"code": "3031", "suburb": "Footscray", "state": "VIC"},
            {"code": "3100", "suburb": "Hawthorn", "state": "VIC"},
            {"code": "4000", "suburb": "Brisbane CBD", "state": "QLD"},
            {"code": "6000", "suburb": "Perth CBD", "state": "WA"},
        ]

        postcodes: List[Postcode] = []
        for pc in postcodes_data:
            p = Postcode(code=pc["code"], suburb=pc["suburb"], state=pc["state"])
            postcodes.append(p)
            logger.debug(f"Generated Postcode: {asdict(p)}")
        
        # Sample providers with zones
        providers = ["FP_1", "FP_2", "FP_3", "FP_4", "FP_5"]

        provider_zones: List[ProviderZone] = []
        zone_templates = {
            "FP_1": [
                ("SYD_CBD", ["2000", "2001"], "metro"),
                ("SYD_INNER", ["2010", "2015"], "inner"),
                ("SYD_NORTH", ["2050", "2060"], "north"),
                ("SYD_REGION", ["2100", "2150"], "regional"),
            ],
            "FP_2": [
                ("MEL_CBD", ["3000", "3001"], "metro"),
                ("MEL_INNER", ["3010", "3020"], "inner"),
                ("MEL_SOUTH", ["3031", "3035"], "south"),
                ("MEL_REGION", ["3100", "3150"], "regional"),
            ],
            "FP_3": [
                ("BNE_CBD", ["4000", "4001"], "metro"),
                ("BNE_SOUTH", ["4010", "4020"], "south"),
                ("BNE_NORTH", ["4050", "4060"], "north"),
                ("BNE_REGION", ["4100", "4200"], "regional"),
            ],
            "FP_4": [
                ("PER_CBD", ["6000", "6001"], "metro"),
                ("PER_INNER", ["6010", "6020"], "inner"),
                ("PER_SOUTH", ["6050", "6060"], "south"),
                ("PER_REGION", ["6100", "6200"], "regional"),
            ],
            "FP_5": [
                ("NSW_EXP", ["2000", "2100"], "express"),
                ("VIC_EXP", ["3000", "3100"], "express"),
                ("QLD_EXP", ["4000", "4100"], "express"),
                ("WA_EXP", ["6000", "6100"], "express"),
            ],
        }

        for provider_id, zones in zone_templates.items():
            for zone_name, postcodes_in_zone, service_type in zones:
                pz = ProviderZone(
                    providerId=provider_id,
                    zoneCode=zone_name,
                    state="NSW",
                    postcodes=postcodes_in_zone,
                    category=service_type
                )
                provider_zones.append(pz)
                logger.debug(f"Added zone: {provider_id}/{zone_name}")

        # Sample routes with pricing/ETD
        routes: List[ProviderZoneRoute] = []

        # FP_1 routes (NSW)
        fp1_routes = [
            ("FP_1", "SYD_CBD", "SYD_INNER", 15.0, 0.5, 2.0, 1000),
            ("FP_1", "SYD_CBD", "SYD_NORTH", 20.0, 0.6, 3.5, 1000),
            ("FP_1", "SYD_INNER", "SYD_NORTH", 12.0, 0.4, 2.5, 1000),
            ("FP_1", "SYD_NORTH", "SYD_REGION", 25.0, 0.7, 4.0, 800),
        ]

        # FP_2 routes (VIC)
        fp2_routes = [
            ("FP_2", "MEL_CBD", "MEL_INNER", 12.0, 0.5, 2.0, 1000),
            ("FP_2", "MEL_CBD", "MEL_SOUTH", 18.0, 0.6, 3.0, 1000),
            ("FP_2", "MEL_INNER", "MEL_SOUTH", 10.0, 0.4, 2.5, 1000),
            ("FP_2", "MEL_SOUTH", "MEL_REGION", 22.0, 0.7, 4.5, 800),
        ]

        # FP_3 routes (QLD)
        fp3_routes = [
            ("FP_3", "BNE_CBD", "BNE_SOUTH", 14.0, 0.5, 2.0, 1000),
            ("FP_3", "BNE_CBD", "BNE_NORTH", 16.0, 0.6, 2.5, 1000),
            ("FP_3", "BNE_SOUTH", "BNE_NORTH", 20.0, 0.7, 3.5, 1000),
            ("FP_3", "BNE_NORTH", "BNE_REGION", 24.0, 0.8, 4.5, 800),
        ]

        # FP_4 routes (WA)
        fp4_routes = [
            ("FP_4", "PER_CBD", "PER_INNER", 13.0, 0.5, 1.5, 1000),
            ("FP_4", "PER_CBD", "PER_SOUTH", 17.0, 0.6, 2.5, 1000),
            ("FP_4", "PER_INNER", "PER_SOUTH", 11.0, 0.4, 2.0, 1000),
            ("FP_4", "PER_SOUTH", "PER_REGION", 20.0, 0.7, 4.0, 800),
        ]

        # FP_5 express routes (interstate)
        fp5_routes = [
            ("FP_5", "NSW_EXP", "VIC_EXP", 50.0, 1.0, 12.0, 500),
            ("FP_5", "VIC_EXP", "QLD_EXP", 55.0, 1.1, 14.0, 500),
            ("FP_5", "QLD_EXP", "WA_EXP", 80.0, 1.5, 24.0, 400),
            ("FP_5", "NSW_EXP", "QLD_EXP", 60.0, 1.2, 18.0, 500),
        ]

        all_routes = fp1_routes + fp2_routes + fp3_routes + fp4_routes + fp5_routes

        for provider_id, from_zone, to_zone, base_cost, cost_per_kg, etd_hours, max_weight_kg in all_routes:
            # You may need to adjust how serviceType is determined; here we use an empty string as a placeholder
            route = ProviderZoneRoute(
                providerId=provider_id,
                fromZone=from_zone,
                toZone=to_zone,
                serviceType="",  # Set appropriately if available
                baseCharge=base_cost,
                perKGRate=cost_per_kg,
                minCharge=base_cost,  # Or set to a different value if needed
                deliveryHrs=etd_hours,
                maxMass=max_weight_kg
            )
            routes.append(route)
            logger.debug(f"Added route: {provider_id} {from_zone} -> {to_zone} (${base_cost})")

        # Ensure Database and Schema exist before inserting!
        # force_recreate=True ensures we connect to the specific DB context cleanly
        logger.info("Initializing Database and Schema...")
        self.create_database(force_recreate=True)
        self.createSchema()

        # Insert data into TerminusDB
        logger.info(f"Inserting {len(postcodes)} postcodes, {len(provider_zones)} zones, {len(routes)} routes...")
        self._insert_data(postcodes, provider_zones, routes)

        # Build graph index
        graph_index_dict = self._build_graph_index(postcodes, provider_zones, routes)
        aggregated_routes = graph_index_dict['zone_routes_map']

        # Group zones by ProviderID for GraphIndex
        zones_by_provider = {}
        for z in provider_zones:
            if z.providerId not in zones_by_provider:
                zones_by_provider[z.providerId] = []
            zones_by_provider[z.providerId].append(z)

        logger.info("PoC data loaded successfully")
        return graph_index_dict, postcodes, provider_zones, routes, GraphIndex(
            postcodes=postcodes,
            providerZones=zones_by_provider,
            zoneRoutes=aggregated_routes
        )

    def _insert_data(self, postcodes: List[Postcode], provider_zones: List[ProviderZone], routes: List[ProviderZoneRoute]) -> None:
        """Insert data into TerminusDB using JSON documents."""
        try:
            # Prepare JSON-LD documents
            documents = []

            # Add Postcodes
            for pc in postcodes:
                doc = {
                    "@type": "Postcode",
                    "@id": f"Postcode/{pc.code}", 
                    "code": pc.code,
                    "suburb": pc.suburb,
                    "state": pc.state
                }
                documents.append(doc)
            
            # Add ProviderZones
            for zone in provider_zones:
                doc = {
                    "@type": "ProviderZone",
                    "@id": f"ProviderZone/{zone.providerId}_{zone.zoneCode}",
                    "provider_id": zone.providerId,
                    "zone_name": zone.zoneCode,
                    "state": zone.state,
                    "postcodes": zone.postcodes,
                    "category": zone.category
                }
                documents.append(doc)
            
            # Add ProviderZoneRoutes
            for route in routes:
                doc = {
                    "@type": "ProviderZoneRoute",
                    "@id": f"ProviderZoneRoute/{route.providerId}_{route.fromZone}_{route.toZone}",
                    "provider_id": route.providerId,
                    "from_zone": route.fromZone,
                    "to_zone": route.toZone,
                    "service_type": route.serviceType,
                    "base_cost": float(route.baseCharge),
                    "cost_per_kg": float(route.perKGRate),
                    "min_charge": float(route.minCharge),
                    "etd_hours": float(route.deliveryHrs),
                    "max_weight_kg": float(route.maxMass),
                    # Ensure these extra fields are included to match new Schema
                    "max_cbm": float(route.maxCBM),
                    "max_pallets": int(route.maxPallets),
                    "reliability_score": float(route.reliabilityScore),
                    "fuel_levy_pct": float(route.fuelLevyPct)
                }
                documents.append(doc)
            
            # Bulk insert documents
            for doc in documents:
                self.client.insert_document(doc)
            logger.info(f"Inserted {len(documents)} documents into TerminusDB")
        except Exception as e:
            logger.error(f"Error inserting data into TerminusDB: {e}")
            raise
    
    def _build_graph_index(self, postcodes: List[Postcode], provider_zones: List[ProviderZone], routes: List[ProviderZoneRoute]) -> Dict:
        """Build in-memory GraphIndex from loaded data."""

        # Build Postcode Map
        postcode_map = {pc.code: pc for pc in postcodes}

        # Build ProviderZone Map
        zones_by_provider = {}

        for z in provider_zones:
            if z.providerId not in zones_by_provider:
                zones_by_provider[z.providerId] = []
            zones_by_provider[z.providerId].append(z)
        
        # Build Route Graph
        provider_graph = {}
        for route in routes:
            key = f"{route.providerId}/{route.fromZone}"
            if key not in provider_graph:
                provider_graph[key] = []
            provider_graph[key].append(route)

        # Build postcode to zones mapping
        pc_to_zones = {}
        for zone in provider_zones:
            for pc in zone.postcodes:
                if pc not in pc_to_zones:
                    pc_to_zones[pc] = []
                pc_to_zones[pc].append(f"{zone.providerId}/{zone.zoneCode}")
        
        # Construct GraphIndex dict
        graph_index = GraphIndex(postcodes=postcodes, providerZones=zones_by_provider, zoneRoutes=provider_graph)
        
        logger.info(f"Built GraphIndex with {len(postcode_map)} postcodes, "f"{len(zones_by_provider)} zones, {len(routes)} routes")
        return {
            "postcodes": {k: v.__dict__ for k, v in postcode_map.items()},
            "provider_zones": {k: [z.__dict__ for z in v ]for k, v in zones_by_provider.items()},
            "routes": [r.__dict__ for r in routes],
            "zone_routes_map": provider_graph,
            "pc_to_zones": pc_to_zones,
            "timestamp": datetime.now().isoformat()
        }

    def load_from_rdbms(self) -> Tuple[Dict, GraphIndex]:
        """
        Load freight network data from SQL Server RDBMS.

        Expects tables:
        - fpzones: Postcode definitions
        - fp_pricing_rules: Zone-to-zone routes
        - fpcosts, fpserviceetds, fpvehicles: Pricing/timing data

        Returns:
            Tuple of (graph_index_dict, GraphIndex)
        """
        try:
            import pyodbc
        except ImportError:
            raise ImportError("pyodbc not installed. Install with: pip install pyodbc")

        logger.info("Loading data from RDBMS...")

        db_url = os.getenv("DATABASE_URL")
        if not db_url:
            raise ValueError("DATABASE_URL environment variable not set")

        conn = pyodbc.connect(db_url)
        cursor = conn.cursor()

        try:
            # Load postcodes
            cursor.execute("SELECT PostalCode, Suburb, State FROM fpzones")
            postcodes = [
                Postcode(code=row[0], suburb=row[1], state=row[2])
                for row in cursor.fetchall()
            ]
            logger.info(f"Loaded {len(postcodes)} postcodes from RDBMS")

            # Load provider zones
            cursor.execute("""
                SELECT provider_id, zone_name, service_type, 
                       STRING_AGG(postal_code, ',') as postcodes
                FROM fp_pricing_rules
                GROUP BY provider_id, zone_name, service_type
            """)
            provider_zones = [
                ProviderZone(
                    providerId=row[0],
                    zoneCode=row[1],
                    category=row[2],
                    state="NSW",
                    postcodes=row[3].split(',')
                )
                for row in cursor.fetchall()
            ]
            logger.info(f"Loaded {len(provider_zones)} provider zones from RDBMS")

            # Load routes with pricing/timing
            cursor.execute("""
                SELECT 
                    p.provider_id, p.from_zone, p.to_zone,
                    c.base_cost, c.cost_per_kg,
                    e.etd_hours,
                    v.max_weight_kg
                FROM fp_pricing_rules p
                JOIN fpcosts c ON p.route_id = c.route_id
                JOIN fpserviceetds e ON p.route_id = e.route_id
                JOIN fpvehicles v ON p.provider_id = v.provider_id
            """)
            routes = [
                ProviderZoneRoute(
                    providerId=row[0],
                    fromZone=row[1],
                    toZone=row[2],
                    serviceType="",  # Set appropriately if available from your data
                    baseCharge=float(row[3]),
                    perKGRate=float(row[4]),
                    minCharge=float(row[3]),  # Or set to a different value if needed
                    deliveryHrs=float(row[5]),
                    maxMass=float(row[6])
                )
                for row in cursor.fetchall()
            ]
            logger.info(f"Loaded {len(routes)} routes from RDBMS")

            # Build graph index
            graph_index_dict = self._build_graph_index(postcodes, provider_zones, routes)
            aggregated_routes = graph_index_dict['zone_routes_map']

            # Insert into TerminusDB
            logger.info("Inserting RDBMS data into TerminusDB...")
            self._insert_data(postcodes, provider_zones, routes)

            zones_by_provider = {}
            for z in provider_zones:
                if z.providerId not in zones_by_provider:
                    zones_by_provider[z.providerId] = []
                zones_by_provider[z.providerId].append(z)

            return graph_index_dict, GraphIndex(
                postcodes=postcodes,
                providerZones=zones_by_provider,
                zoneRoutes=aggregated_routes
            )

        finally:
            cursor.close()
            conn.close()
    
    def export_graph_json(self, filepath: str = "graph_data.json") -> str:
        """
        Export graph data from TerminusDB to JSON file.

        Args:
            filepath: Output file path

        Returns:
            Path to exported file
        """
        try:
            self.client.connect(db=self.dbName)

            logger.info(f"Exporting graph to {filepath}...")

            # Query all documents
            woql = WOQLQuery().triple("X", "Y", "Z").select("X", "Y", "Z")
            safe_woql: Union[WOQLQuery, Dict[Any, Any]]

            if isinstance(woql, WOQLQuery):
                safe_woql = woql
            else:
                safe_woql = {}

            result = self.client.query(safe_woql)

            # Save to file
            with open(filepath, 'w') as f:
                json.dump(result, f, indent=2)

            logger.info(f"Graph exported successfully to {filepath}")
            return filepath

        except Exception as e:
            logger.error(f"Error exporting graph: {e}")
            raise