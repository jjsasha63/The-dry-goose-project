
from __future__ import annotations

import os
import json
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import (
    col, lit, when, isnan, isnull, mean, stddev, percentile_approx,
    row_number, rank, dense_rank, abs as spark_abs, sqrt, pow as spark_pow
)
from pyspark.sql.window import Window
from pyspark.ml import Pipeline, PipelineModel, Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol, Param, Params
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.ml.feature import (
    VectorAssembler, StandardScaler, StringIndexer, OneHotEncoder,
    PCA, Imputer, QuantileDiscretizer, Bucketizer
)
from pyspark.ml.clustering import KMeans, BisectingKMeans
from pyspark.ml.stat import Summarizer
from pyspark.ml.linalg import Vectors, VectorUDT
import pyspark.sql.functions as F


@dataclass
class AnomalyConfig:
    """Configuration for anomaly detection - fully serializable."""
    contamination: float = 0.05
    n_clusters: int = 8
    statistical_threshold: float = 3.0
    distance_threshold_percentile: float = 95.0
    ensemble_weights: Dict[str, float] = None
    
    def __post_init__(self):
        if self.ensemble_weights is None:
            self.ensemble_weights = {
                'cluster_distance': 0.3,
                'statistical_outlier': 0.3,
                'isolation_score': 0.2,
                'density_score': 0.2
            }


class StatisticalOutlierTransformer(Transformer, HasInputCol, HasOutputCol, 
                                   DefaultParamsReadable, DefaultParamsWritable):
    """Pure Spark ML transformer for statistical outlier detection."""
    
    threshold = Param(Params._dummy(), "threshold", "Statistical threshold for outliers")
    
    def __init__(self, inputCol: str = "features", outputCol: str = "statistical_score", 
                 threshold: float = 3.0):
        super().__init__()
        self._setDefault(threshold=3.0)
        self._set(inputCol=inputCol, outputCol=outputCol, threshold=threshold)
    
    def setThreshold(self, value: float):
        return self._set(threshold=value)
    
    def getThreshold(self):
        return self.getOrDefault(self.threshold)
    
    def _transform(self, dataset: DataFrame) -> DataFrame:
        input_col = self.getInputCol()
        output_col = self.getOutputCol()
        threshold = self.getThreshold()
        
        # Calculate feature-wise statistics using Summarizer
        stats = dataset.select(
            Summarizer.mean(col(input_col)).alias("mean_vector"),
            Summarizer.std(col(input_col)).alias("std_vector")
        ).collect()[0]
        
        mean_vals = stats["mean_vector"].toArray()
        std_vals = stats["std_vector"].toArray()
        
        # Create UDF for z-score calculation
        from pyspark.sql.functions import udf
        from pyspark.sql.types import DoubleType
        
        def calculate_z_score(features):
            if features is None:
                return 0.0
            z_scores = []
            for i, val in enumerate(features.toArray()):
                if std_vals[i] > 0:
                    z_score = abs(val - mean_vals[i]) / std_vals[i]
                    z_scores.append(z_score)
            return max(z_scores) if z_scores else 0.0
        
        z_score_udf = udf(calculate_z_score, DoubleType())
        
        return dataset.withColumn(output_col, z_score_udf(col(input_col)))


class ClusterDistanceTransformer(Transformer, HasInputCol, HasOutputCol,
                                DefaultParamsReadable, DefaultParamsWritable):
    """Transformer that adds cluster distance scores."""
    
    clusterCenters = Param(Params._dummy(), "clusterCenters", "Cluster centers")
    
    def __init__(self, inputCol: str = "features", outputCol: str = "cluster_distance"):
        super().__init__()
        self._set(inputCol=inputCol, outputCol=outputCol)
    
    def setClusterCenters(self, centers):
        return self._set(clusterCenters=centers)
    
    def getClusterCenters(self):
        return self.getOrDefault(self.clusterCenters)
    
    def _transform(self, dataset: DataFrame) -> DataFrame:
        input_col = self.getInputCol()
        output_col = self.getOutputCol()
        
        # This would be set during pipeline fitting
        if not self.isDefined(self.clusterCenters):
            raise ValueError("Cluster centers must be set before transformation")
        
        centers = self.getClusterCenters()
        
        from pyspark.sql.functions import udf
        from pyspark.sql.types import DoubleType
        import math
        
        def min_distance_to_centers(features):
            if features is None:
                return float('inf')
            min_dist = float('inf')
            feature_array = features.toArray()
            for center in centers:
                dist = sum((feature_array[i] - center[i]) ** 2 for i in range(len(feature_array)))
                min_dist = min(min_dist, math.sqrt(dist))
            return min_dist
        
        distance_udf = udf(min_distance_to_centers, DoubleType())
        return dataset.withColumn(output_col, distance_udf(col(input_col)))


class IsolationScoreTransformer(Transformer, HasInputCol, HasOutputCol,
                               DefaultParamsReadable, DefaultParamsWritable):
    """Simplified isolation forest score using statistical approximation."""
    
    def __init__(self, inputCol: str = "features", outputCol: str = "isolation_score"):
        super().__init__()
        self._set(inputCol=inputCol, outputCol=outputCol)
    
    def _transform(self, dataset: DataFrame) -> DataFrame:
        input_col = self.getInputCol()
        output_col = self.getOutputCol()
        
        # Approximate isolation score using feature variance and extremeness
        from pyspark.sql.functions import udf
        from pyspark.sql.types import DoubleType
        
        # Calculate global statistics first
        stats = dataset.select(
            Summarizer.mean(col(input_col)).alias("mean_vector"),
            Summarizer.std(col(input_col)).alias("std_vector"),
            Summarizer.min(col(input_col)).alias("min_vector"),
            Summarizer.max(col(input_col)).alias("max_vector")
        ).collect()[0]
        
        mean_vals = stats["mean_vector"].toArray()
        std_vals = stats["std_vector"].toArray()
        min_vals = stats["min_vector"].toArray()
        max_vals = stats["max_vector"].toArray()
        
        def calculate_isolation_score(features):
            if features is None:
                return 0.0
            
            feature_array = features.toArray()
            isolation_factors = []
            
            for i, val in enumerate(feature_array):
                # Measure how extreme the value is in its distribution
                if std_vals[i] > 0:
                    # Distance from mean in standard deviations
                    z_score = abs(val - mean_vals[i]) / std_vals[i]
                    
                    # Position in the range (0=min, 1=max)
                    if max_vals[i] != min_vals[i]:
                        range_position = (val - min_vals[i]) / (max_vals[i] - min_vals[i])
                        extremeness = min(range_position, 1 - range_position)
                    else:
                        extremeness = 0.0
                    
                    # Combine z-score and extremeness
                    isolation_factor = z_score * (1 - extremeness)
                    isolation_factors.append(isolation_factor)
            
            return sum(isolation_factors) / len(isolation_factors) if isolation_factors else 0.0
        
        isolation_udf = udf(calculate_isolation_score, DoubleType())
        return dataset.withColumn(output_col, isolation_udf(col(input_col)))


class DensityScoreTransformer(Transformer, HasInputCol, HasOutputCol,
                             DefaultParamsReadable, DefaultParamsWritable):
    """Local density estimation using k-nearest neighbors approximation."""
    
    k = Param(Params._dummy(), "k", "Number of neighbors for density estimation")
    
    def __init__(self, inputCol: str = "features", outputCol: str = "density_score", k: int = 10):
        super().__init__()
        self._setDefault(k=10)
        self._set(inputCol=inputCol, outputCol=outputCol, k=k)
    
    def setK(self, value: int):
        return self._set(k=value)
    
    def getK(self):
        return self.getOrDefault(self.k)
    
    def _transform(self, dataset: DataFrame) -> DataFrame:
        input_col = self.getInputCol()
        output_col = self.getOutputCol()
        k = self.getK()
        
        # Approximate density using distance to k-th nearest cluster center
        # This is a simplified approach suitable for large-scale processing
        
        # Use percentile-based density estimation
        from pyspark.sql.functions import udf
        from pyspark.sql.types import DoubleType
        
        # Calculate feature-wise percentiles for density estimation
        percentiles = [0.1, 0.25, 0.5, 0.75, 0.9]
        
        stats = dataset.select(
            Summarizer.mean(col(input_col)).alias("mean_vector")
        ).collect()[0]
        
        mean_vals = stats["mean_vector"].toArray()
        
        def calculate_density_score(features):
            if features is None:
                return 1.0  # High density score (low density)
            
            feature_array = features.toArray()
            
            # Calculate distance from feature centroid
            centroid_distance = sum((val - mean_vals[i]) ** 2 for i, val in enumerate(feature_array))
            centroid_distance = centroid_distance ** 0.5
            
            # Convert distance to density score (higher distance = lower density = higher score)
            density_score = min(centroid_distance, 10.0) / 10.0  # Normalize to [0, 1]
            
            return density_score
        
        density_udf = udf(calculate_density_score, DoubleType())
        return dataset.withColumn(output_col, density_udf(col(input_col)))


class StatelessAnomalyDetectionPipeline:
    """
    Stateless anomaly detection pipeline using pure Spark ML components.
    No threading objects or custom state - completely serializable.
    """
    
    @staticmethod
    def create_preprocessing_pipeline(df: DataFrame, 
                                    config: AnomalyConfig) -> Pipeline:
        """Create preprocessing pipeline for feature engineering."""
        
        # Identify column types
        numeric_cols = []
        categorical_cols = []
        
        for field in df.schema.fields:
            if isinstance(field.dataType, (IntegerType, DoubleType)):
                numeric_cols.append(field.name)
            else:
                categorical_cols.append(field.name)
        
        stages = []
        
        # Handle missing values in numeric columns
        if numeric_cols:
            imputer = Imputer(
                inputCols=numeric_cols,
                outputCols=[f"{col}_imputed" for col in numeric_cols],
                strategy="mean"
            )
            stages.append(imputer)
            numeric_cols = [f"{col}_imputed" for col in numeric_cols]
        
        # Process categorical columns
        encoded_categorical_cols = []
        for col_name in categorical_cols:
            # String indexing
            indexer = StringIndexer(
                inputCol=col_name,
                outputCol=f"{col_name}_indexed",
                handleInvalid="keep"
            )
            stages.append(indexer)
            
            # One-hot encoding
            encoder = OneHotEncoder(
                inputCol=f"{col_name}_indexed",
                outputCol=f"{col_name}_encoded"
            )
            stages.append(encoder)
            encoded_categorical_cols.append(f"{col_name}_encoded")
        
        # Combine all features
        all_feature_cols = numeric_cols + encoded_categorical_cols
        if all_feature_cols:
            assembler = VectorAssembler(
                inputCols=all_feature_cols,
                outputCol="raw_features",
                handleInvalid="keep"
            )
            stages.append(assembler)
            
            # Scale features
            scaler = StandardScaler(
                inputCol="raw_features",
                outputCol="scaled_features",
                withStd=True,
                withMean=True
            )
            stages.append(scaler)
        else:
            raise ValueError("No valid columns found for feature extraction")
        
        return Pipeline(stages=stages)
    
    @staticmethod
    def create_anomaly_detection_pipeline(config: AnomalyConfig) -> Pipeline:
        """Create anomaly detection pipeline using ensemble of methods."""
        
        stages = []
        
        # 1. Clustering for distance-based anomalies
        kmeans = KMeans(
            k=config.n_clusters,
            featuresCol="scaled_features",
            predictionCol="cluster",
            seed=42
        )
        stages.append(kmeans)
        
        # 2. Statistical outlier detection
        statistical_transformer = StatisticalOutlierTransformer(
            inputCol="scaled_features",
            outputCol="statistical_score",
            threshold=config.statistical_threshold
        )
        stages.append(statistical_transformer)
        
        # 3. Isolation score approximation
        isolation_transformer = IsolationScoreTransformer(
            inputCol="scaled_features",
            outputCol="isolation_score"
        )
        stages.append(isolation_transformer)
        
        # 4. Density score
        density_transformer = DensityScoreTransformer(
            inputCol="scaled_features",
            outputCol="density_score",
            k=min(10, config.n_clusters)
        )
        stages.append(density_transformer)
        
        return Pipeline(stages=stages)
    
    @staticmethod
    def add_cluster_distance_score(df: DataFrame, 
                                 fitted_kmeans, 
                                 config: AnomalyConfig) -> DataFrame:
        """Add cluster distance scores after K-means fitting."""
        
        centers = fitted_kmeans.clusterCenters()
        
        from pyspark.sql.functions import udf
        from pyspark.sql.types import DoubleType
        import math
        
        def min_distance_to_centers(features, cluster_id):
            if features is None:
                return float('inf')
            
            feature_array = features.toArray()
            center = centers[int(cluster_id)]
            
            distance = sum((feature_array[i] - center[i]) ** 2 for i in range(len(feature_array)))
            return math.sqrt(distance)
        
        distance_udf = udf(min_distance_to_centers, DoubleType())
        
        return df.withColumn("cluster_distance", 
                           distance_udf(col("scaled_features"), col("cluster")))
    
    @staticmethod
    def calculate_ensemble_score(df: DataFrame, config: AnomalyConfig) -> DataFrame:
        """Calculate final ensemble anomaly score."""
        
        weights = config.ensemble_weights
        
        # Normalize individual scores to [0, 1] range
        score_columns = ['cluster_distance', 'statistical_score', 'isolation_score', 'density_score']
        
        for score_col in score_columns:
            if score_col in df.columns:
                # Get min/max for normalization
                min_max = df.select(
                    F.min(score_col).alias("min_val"),
                    F.max(score_col).alias("max_val")
                ).collect()[0]
                
                min_val = min_max["min_val"]
                max_val = min_max["max_val"]
                
                if max_val > min_val:
                    df = df.withColumn(f"{score_col}_normalized",
                                     (col(score_col) - min_val) / (max_val - min_val))
                else:
                    df = df.withColumn(f"{score_col}_normalized", lit(0.0))
        
        # Calculate weighted ensemble score
        ensemble_expr = lit(0.0)
        for score_col, weight in weights.items():
            normalized_col = f"{score_col}_normalized"
            if normalized_col in df.columns:
                ensemble_expr = ensemble_expr + (col(normalized_col) * weight)
        
        df = df.withColumn("ensemble_score", ensemble_expr)
        
        # Determine anomaly threshold
        threshold = df.approxQuantile("ensemble_score", [1.0 - config.contamination], 0.01)[0]
        
        # Create binary predictions
        df = df.withColumn("anomaly_prediction",
                         when(col("ensemble_score") > threshold, 1).otherwise(0))
        
        return df


class StatelessAnomalyDetector:
    """
    Main interface for stateless anomaly detection.
    All operations are functional - no mutable state stored.
    """
    
    def __init__(self, config: Optional[AnomalyConfig] = None):
        self.config = config or AnomalyConfig()
    
    def fit(self, df: DataFrame) -> Tuple[PipelineModel, PipelineModel, Dict[str, Any]]:
        """
        Fit preprocessing and anomaly detection pipelines.
        
        Returns:
            preprocessing_model: Fitted preprocessing pipeline
            anomaly_model: Fitted anomaly detection pipeline  
            meta Model metadata
        """
        
        # Create and fit preprocessing pipeline
        preprocessing_pipeline = StatelessAnomalyDetectionPipeline.create_preprocessing_pipeline(
            df, self.config
        )
        preprocessing_model = preprocessing_pipeline.fit(df)
        
        # Transform data for anomaly detection
        preprocessed_df = preprocessing_model.transform(df)
        
        # Create and fit anomaly detection pipeline
        anomaly_pipeline = StatelessAnomalyDetectionPipeline.create_anomaly_detection_pipeline(
            self.config
        )
        anomaly_model = anomaly_pipeline.fit(preprocessed_df)
        
        # Create metadata
        metadata = {
            "creation_date": datetime.now().isoformat(),
            "config": asdict(self.config),
            "feature_names": df.columns,
            "n_samples_trained": df.count(),
            "preprocessing_stages": len(preprocessing_model.stages),
            "anomaly_detection_stages": len(anomaly_model.stages)
        }
        
        return preprocessing_model, anomaly_model, metadata
    
    def predict(self, df: DataFrame, 
                preprocessing_model: PipelineModel,
                anomaly_model: PipelineModel,
                meta Dict[str, Any]) -> DataFrame:
        """
        Predict anomalies using fitted models.
        
        Returns:
            DataFrame with anomaly predictions and scores
        """
        
        # Apply preprocessing
        preprocessed_df = preprocessing_model.transform(df)
        
        # Apply anomaly detection
        anomaly_df = anomaly_model.transform(preprocessed_df)
        
        # Get the fitted K-means model for distance calculation
        kmeans_model = None
        for stage in anomaly_model.stages:
            if hasattr(stage, 'clusterCenters'):
                kmeans_model = stage
                break
        
        if kmeans_model:
            # Add cluster distance scores
            anomaly_df = StatelessAnomalyDetectionPipeline.add_cluster_distance_score(
                anomaly_df, kmeans_model, self.config
            )
        
        # Calculate final ensemble scores
        result_df = StatelessAnomalyDetectionPipeline.calculate_ensemble_score(
            anomaly_df, self.config
        )
        
        # Select output columns
        output_cols = df.columns + [
            "anomaly_prediction", "ensemble_score", 
            "cluster_distance", "statistical_score", 
            "isolation_score", "density_score"
        ]
        
        return result_df.select(*[col for col in output_cols if col in result_df.columns])


class ModelPersistence:
    """Utility functions for saving and loading models."""
    
    @staticmethod
    def save_models(preprocessing_model: PipelineModel,
                   anomaly_model: PipelineModel,
                   meta Dict[str, Any],
                   config: AnomalyConfig,
                   path: str) -> str:
        """Save all models and metadata."""
        
        os.makedirs(path, exist_ok=True)
        
        # Save Spark ML models
        preprocessing_path = os.path.join(path, "preprocessing_model")
        preprocessing_model.write().overwrite().save(preprocessing_path)
        
        anomaly_path = os.path.join(path, "anomaly_model")
        anomaly_model.write().overwrite().save(anomaly_path)
        
        # Save configuration and metadata
        config_path = os.path.join(path, "config.json")
        with open(config_path, 'w') as f:
            json.dump(asdict(config), f, indent=2)
        
        metadata_path = os.path.join(path, "metadata.json")
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        print(f"Models saved to: {path}")
        return path
    
    @staticmethod
    def load_models(path: str) -> Tuple[PipelineModel, PipelineModel, Dict[str, Any], AnomalyConfig]:
        """Load all models and metadata."""
        
        if not os.path.exists(path):
            raise FileNotFoundError(f"Model directory not found: {path}")
        
        # Load Spark ML models
        preprocessing_path = os.path.join(path, "preprocessing_model")
        preprocessing_model = PipelineModel.load(preprocessing_path)
        
        anomaly_path = os.path.join(path, "anomaly_model")
        anomaly_model = PipelineModel.load(anomaly_path)
        
        # Load configuration
        config_path = os.path.join(path, "config.json")
        with open(config_path, 'r') as f:
            config_dict = json.load(f)
        config = AnomalyConfig(**config_dict)
        
        # Load metadata
        metadata_path = os.path.join(path, "metadata.json")
        with open(metadata_path, 'r') as f:
            metadata = json.load(f)
        
        print(f"Models loaded from: {path}")
        return preprocessing_model, anomaly_model, metadata, config


# Example usage
if __name__ == "__main__":
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("StatelessAnomalyDetection") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    # Create sample data
    from pyspark.sql.types import StructType, StructField, DoubleType, StringType
    import numpy as np
    
    schema = StructType([
        StructField("feature1", DoubleType(), True),
        StructField("feature2", DoubleType(), True),
        StructField("category", StringType(), True),
        StructField("feature3", DoubleType(), True)
    ])
    
    # Generate sample data
    np.random.seed(42)
    normal_data = [(float(np.random.normal(0, 1)), float(np.random.normal(0, 1)), 
                   np.random.choice(['A', 'B', 'C']), float(np.random.uniform(-2, 2)))
                  for _ in range(950)]
    
    anomaly_data = [(float(np.random.normal(5, 1)), float(np.random.normal(-5, 1)), 
                    np.random.choice(['D', 'E']), float(np.random.uniform(5, 10)))
                   for _ in range(50)]
    
    all_data = normal_data + anomaly_data
    df = spark.createDataFrame(all_data, schema)
    
    print("Sample ")
    df.show(5)
    
    # Configure and train detector
    config = AnomalyConfig(contamination=0.05, n_clusters=8)
    detector = StatelessAnomalyDetector(config)
    
    print("Training anomaly detector...")
    preprocessing_model, anomaly_model, metadata = detector.fit(df)
    
    print("Making predictions...")
    results = detector.predict(df, preprocessing_model, anomaly_model, metadata)
    
    # Show results
    print("\nSample predictions:")
    results.select("feature1", "feature2", "category", "anomaly_prediction", "ensemble_score").show(10)
    
    # Get summary statistics
    total_count = results.count()
    anomaly_count = results.filter(col("anomaly_prediction") == 1).count()
    anomaly_rate = anomaly_count / total_count
    
    print(f"\nAnomaly Detection Summary:")
    print(f"Total samples: {total_count}")
    print(f"Anomalies detected: {anomaly_count}")
    print(f"Anomaly rate: {anomaly_rate:.3f}")
    
    # Save models
    model_path = "./stateless_anomaly_model"
    ModelPersistence.save_models(
        preprocessing_model, anomaly_model, metadata, config, model_path
    )
    
    # Load models (demonstration)
    print("\nLoading saved models...")
    loaded_preprocessing, loaded_anomaly, loaded_metadata, loaded_config = \
        ModelPersistence.load_models(model_path)
    
    # Test loaded models
    loaded_detector = StatelessAnomalyDetector(loaded_config)
    test_results = loaded_detector.predict(
        df.limit(5), loaded_preprocessing, loaded_anomaly, loaded_metadata
    )
    
    print("Loaded model predictions:")
    test_results.select("feature1", "category", "anomaly_prediction", "ensemble_score").show()
    
    spark.stop()
