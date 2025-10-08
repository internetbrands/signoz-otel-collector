package migrate

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/SigNoz/signoz-otel-collector/cmd/signozotelcollector/config"
	schemamigrator "github.com/SigNoz/signoz-otel-collector/cmd/signozschemamigrator/schema_migrator"
	"github.com/SigNoz/signoz-otel-collector/constants"
	"github.com/cenkalti/backoff/v4"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

type syncCheck struct {
	conn             clickhouse.Conn
	timeout          time.Duration
	migrationManager *schemamigrator.MigrationManager
	dbNames          schemamigrator.DatabaseNames
	logger           *zap.Logger
}

func registerSyncCheck(parentCmd *cobra.Command, logger *zap.Logger) {
	syncCheckCommand := &cobra.Command{
		Use:          "check",
		Short:        "Checks the status of migrations for the store by checking the status of migrations in the migration table.",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			dbNames := schemamigrator.DatabaseNames{
				Traces:    config.Clickhouse.TraceDatabase,
				Logs:      config.Clickhouse.LogDatabase,
				Metrics:   config.Clickhouse.MetricsDatabase,
				Metadata:  config.Clickhouse.MetadataDatabase,
				Analytics: config.Clickhouse.AnalyticsDatabase,
				Meter:     config.Clickhouse.MeterDatabase,
			}
			check, err := newSyncCheck(config.Clickhouse.DSN, config.Clickhouse.Cluster, config.Clickhouse.Replication, config.MigrateSyncCheck.Timeout, dbNames, logger)
			if err != nil {
				return err
			}

			err = check.Run(cmd.Context())
			if err != nil {
				return err
			}

			return nil
		},
	}

	config.MigrateSyncCheck.RegisterFlags(syncCheckCommand)

	parentCmd.AddCommand(syncCheckCommand)
}

func newSyncCheck(dsn string, cluster string, replication bool, timeout time.Duration, dbNames schemamigrator.DatabaseNames, logger *zap.Logger) (*syncCheck, error) {
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, err
	}

	migrationManager, err := schemamigrator.NewMigrationManager(
		schemamigrator.WithClusterName(cluster),
		schemamigrator.WithReplicationEnabled(replication),
		schemamigrator.WithConn(conn),
		schemamigrator.WithConnOptions(*opts),
		schemamigrator.WithLogger(logger),
		schemamigrator.WithDatabaseNames(dbNames),
	)
	if err != nil {
		return nil, err
	}

	return &syncCheck{
		conn:             conn,
		timeout:          timeout,
		migrationManager: migrationManager,
		dbNames:          dbNames,
		logger:           logger,
	}, nil
}

func (cmd *syncCheck) Run(ctx context.Context) error {
	backoff := backoff.NewExponentialBackOff()
	backoff.MaxElapsedTime = cmd.timeout

	for {
		err := cmd.Check(ctx)
		if err == nil {
			break
		}

		cmd.logger.Info("Error occurred while checking for sync migrations to complete, retrying", zap.Error(err))
		nextBackOff := backoff.NextBackOff()
		if nextBackOff == backoff.Stop {
			return errors.New("timed out waiting for sync migrations to complete within the configured timeout")
		}
		time.Sleep(nextBackOff)
	}

	return nil
}

func (cmd *syncCheck) Check(ctx context.Context) error {
	tracesLastSyncMigrationID, err := cmd.getLastSyncMigration(schemamigrator.TracesMigrations)
	if err == nil {
		ok, err := cmd.migrationManager.CheckMigrationStatus(ctx, cmd.dbNames.Traces, tracesLastSyncMigrationID, schemamigrator.FinishedStatus)
		if err != nil {
			return err
		}

		if !ok {
			return fmt.Errorf("migration with ID %d for database '%s' has not been completed", tracesLastSyncMigrationID, cmd.dbNames.Traces)
		}
	}

	logsMigrations := schemamigrator.LogsMigrations
	if constants.EnableLogsMigrationsV2 {
		logsMigrations = schemamigrator.LogsMigrationsV2
	}

	logsLastSyncMigrationID, err := cmd.getLastSyncMigration(logsMigrations)
	if err == nil {
		ok, err := cmd.migrationManager.CheckMigrationStatus(ctx, cmd.dbNames.Logs, logsLastSyncMigrationID, schemamigrator.FinishedStatus)
		if err != nil {
			return err
		}

		if !ok {
			return fmt.Errorf("migration with ID %d for database '%s' has not been completed", logsLastSyncMigrationID, cmd.dbNames.Logs)
		}
	}

	metricsLastSyncMigrationID, err := cmd.getLastSyncMigration(schemamigrator.MetricsMigrations)
	if err == nil {
		ok, err := cmd.migrationManager.CheckMigrationStatus(ctx, cmd.dbNames.Metrics, metricsLastSyncMigrationID, schemamigrator.FinishedStatus)
		if err != nil {
			return err
		}

		if !ok {
			return fmt.Errorf("migration with ID %d for database '%s' has not been completed", metricsLastSyncMigrationID, cmd.dbNames.Metrics)
		}
	}

	metadataLastSyncMigrationID, err := cmd.getLastSyncMigration(schemamigrator.MetadataMigrations)
	if err == nil {
		ok, err := cmd.migrationManager.CheckMigrationStatus(ctx, cmd.dbNames.Metadata, metadataLastSyncMigrationID, schemamigrator.FinishedStatus)
		if err != nil {
			return err
		}

		if !ok {
			return fmt.Errorf("migration with ID %d for database '%s' has not been completed", metadataLastSyncMigrationID, cmd.dbNames.Metadata)
		}
	}

	analyticsLastSyncMigrationID, err := cmd.getLastSyncMigration(schemamigrator.AnalyticsMigrations)
	if err == nil {
		ok, err := cmd.migrationManager.CheckMigrationStatus(ctx, cmd.dbNames.Analytics, analyticsLastSyncMigrationID, schemamigrator.FinishedStatus)
		if err != nil {
			return err
		}

		if !ok {
			return fmt.Errorf("migration with ID %d for database '%s' has not been completed", analyticsLastSyncMigrationID, cmd.dbNames.Analytics)
		}
	}

	meterLastSyncMigrationID, err := cmd.getLastSyncMigration(schemamigrator.MeterMigrations)
	if err == nil {
		ok, err := cmd.migrationManager.CheckMigrationStatus(ctx, cmd.dbNames.Meter, meterLastSyncMigrationID, schemamigrator.FinishedStatus)
		if err != nil {
			return err
		}

		if !ok {
			return fmt.Errorf("migration with ID %d for database '%s' has not been completed", meterLastSyncMigrationID, cmd.dbNames.Meter)
		}
	}

	return nil
}

func (cmd *syncCheck) getLastSyncMigration(migrations []schemamigrator.SchemaMigrationRecord) (uint64, error) {
	for i := len(migrations) - 1; i >= 0; i-- {
		if cmd.migrationManager.IsSync(migrations[i]) {
			return migrations[i].MigrationID, nil
		}
	}

	return 0, fmt.Errorf("no sync migration found")
}
