#!/usr/bin/env python3
"""
Run all data pipelines in the correct order based on dependencies.

Dependency order:
1. dim_date
2. dim_customer, dim_product, dim_seller (can run in parallel)
3. dim_campaign (depends on dim_customer, dim_product, dim_seller)
4. fact_sales (depends on all dimensions)
5. fact_inventory, fact_cart, fact_marketing (can run in parallel, depend on fact_sales)
"""

import argparse
import importlib
import logging
import sys
from pathlib import Path
from typing import List, Optional

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("pipeline_runner")

# Define pipeline dependencies
PIPELINE_DEPENDENCIES = {
    'dim_date': {
        'module': 'pipelines.dim_date',
        'dependencies': []
    },
    'dim_customer': {
        'module': 'pipelines.dim_customer',
        'dependencies': []
    },
    'dim_product': {
        'module': 'pipelines.dim_product',
        'dependencies': []
    },
    'dim_seller': {
        'module': 'pipelines.dim_seller',
        'dependencies': []
    },
    'dim_campaign': {
        'module': 'pipelines.dim_campaign',
        'dependencies': ['dim_customer', 'dim_product', 'dim_seller']
    },
    'fact_sales': {
        'module': 'pipelines.fact_sales',
        'dependencies': ['dim_date', 'dim_customer', 'dim_product', 'dim_seller']
    },
    'fact_inventory': {
        'module': 'pipelines.fact_inventory',
        'dependencies': ['fact_sales']
    },
    'fact_cart': {
        'module': 'pipelines.fact_cart',
        'dependencies': ['fact_sales']
    },
    'fact_marketing': {
        'module': 'pipelines.fact_marketing',
        'dependencies': ['fact_sales']
    }
}


def run_pipeline(pipeline_name: str, pipelines_to_run: Optional[List[str]] = None) -> bool:
    """Run a single pipeline if it's in the list of pipelines to run."""
    if pipelines_to_run and pipeline_name not in pipelines_to_run:
        logger.info(f"Skipping {pipeline_name} (not in the list of pipelines to run)")
        return True
        
    logger.info(f"Starting {pipeline_name} pipeline...")
    try:
        module_name = PIPELINE_DEPENDENCIES[pipeline_name]['module']
        module = importlib.import_module(module_name)
        module.run()
        logger.info(f"Successfully completed {pipeline_name} pipeline")
        return True
    except Exception as e:
        logger.error(f"Error in {pipeline_name} pipeline: {str(e)}", exc_info=True)
        return False


def run_all_pipelines(pipelines_to_run: Optional[List[str]] = None) -> bool:
    """Run all pipelines in the correct order based on dependencies."""
    # Define the execution order
    execution_order = [
        # First run all dimension pipelines
        ['dim_date'],
        ['dim_customer', 'dim_product', 'dim_seller'],  # Can run in parallel
        ['dim_campaign'],
        # Then fact pipelines
        ['fact_sales'],
        ['fact_inventory', 'fact_cart', 'fact_marketing']  # Can run in parallel
    ]
    
    success = True
    
    for pipeline_group in execution_order:
        group_success = True
        
        # Check if any pipeline in this group should run
        if pipelines_to_run and not any(p in pipelines_to_run for p in pipeline_group):
            logger.info(f"Skipping pipeline group: {', '.join(pipeline_group)}")
            continue
            
        logger.info(f"Starting pipeline group: {', '.join(pipeline_group)}")
        
        # Run all pipelines in the current group
        for pipeline in pipeline_group:
            if not run_pipeline(pipeline, pipelines_to_run):
                group_success = False
                success = False
                logger.error(f"Pipeline {pipeline} failed, but continuing with other pipelines")
        
        if not group_success:
            logger.error(f"One or more pipelines in group {pipeline_group} failed")
    
    return success


def main():
    parser = argparse.ArgumentParser(description='Run data pipelines in the correct order')
    parser.add_argument(
        '--pipelines',
        nargs='+',
        choices=list(PIPELINE_DEPENDENCIES.keys()) + ['all'],
        default=['all'],
        help='List of pipelines to run (default: all)'
    )
    
    args = parser.parse_args()
    
    if 'all' in args.pipelines:
        pipelines_to_run = None  # Run all pipelines
    else:
        pipelines_to_run = args.pipelines
    
    logger.info(f"Starting pipeline execution for: {pipelines_to_run if pipelines_to_run else 'all pipelines'}")
    
    success = run_all_pipelines(pipelines_to_run)
    
    if success:
        logger.info("All pipelines completed successfully!")
        return 0
    else:
        logger.error("One or more pipelines failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
