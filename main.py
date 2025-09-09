#!/usr/bin/env python3
"""
Main entry point for the Fleet Optimization Solution.

This module provides a unified entry point for various components of the system,
handling proper import resolution and path configuration.
"""

import sys
import os
import argparse

# Add the src directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def main():
    """Main entry point that dispatches to appropriate modules."""
    parser = argparse.ArgumentParser(description='Fleet Optimization Solution')
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Training command
    train_parser = subparsers.add_parser('train', help='Train predictive maintenance model')
    train_parser.add_argument('--lookback-days', type=int, default=90,
                            help='Number of days of historical data to use for training')
    train_parser.add_argument('--register-only', action='store_true',
                            help='Only register an existing model, do not train')
    
    # Batch processing command
    batch_parser = subparsers.add_parser('batch-process', help='Run batch processing')
    batch_parser.add_argument('--start-date', help='Start date for processing (YYYY-MM-DD)')
    batch_parser.add_argument('--end-date', help='End date for processing (YYYY-MM-DD)')
    batch_parser.add_argument('--engine', choices=['pandas', 'dask', 'spark'],
                            default='pandas', help='Processing engine to use')
    batch_parser.add_argument('--load-to', choices=['database', 'storage', 'all'],
                            default='all', help='Where to load processed data')
    
    # Stream processing command
    stream_parser = subparsers.add_parser('stream', help='Start stream processing')
    
    args = parser.parse_args()
    
    if args.command == 'train':
        from models.train_predictive_maintenance import PredictiveMaintenanceTrainer
        trainer = PredictiveMaintenanceTrainer()
        if args.register_only:
            version = trainer.ensure_model_exists()
            print(f"Model ensured: version {version}")
        else:
            if args.lookback_days != 90:
                trainer.config['training']['lookback_days'] = args.lookback_days
            results = trainer.train_and_register()
            print(f"Training completed. Best model: {results['best_model_name']}")
    
    elif args.command == 'batch-process':
        from data.batch_processor import BatchProcessor
        processor = BatchProcessor(processing_engine=args.engine)
        
        # Get default dates if not provided
        if args.start_date is None or args.end_date is None:
            start_date, end_date = processor._get_default_dates()
        else:
            start_date, end_date = args.start_date, args.end_date
        
        telemetry_data = processor.extract_telemetry_data(start_date, end_date)
        weather_data = processor.extract_weather_data(start_date, end_date)
        processed_data = processor.transform_and_clean_data(telemetry_data, weather_data)
        processor.load_processed_data(processed_data, args.load_to)
        processor.log_processing_metadata(processed_data, start_date, end_date)
        print("Batch processing completed successfully")
    
    elif args.command == 'stream':
        from data.stream_processor import StreamProcessor
        processor = StreamProcessor()
        processor.run()
    
    else:
        parser.print_help()

if __name__ == "__main__":
    main()