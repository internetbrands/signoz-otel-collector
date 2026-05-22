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

type asyncCheck struct {
	conn             clickhouse.Conn
	timeout          time.Duration
	migrationManager *schemamigrator.MigrationManager
	dbNames          schemamigrator.DatabaseNames
	logger           *zap.Logger
}

func registerAsyncCheck(parentCmd *cobra.Command, logger *zap.Logger) {
	syncCheckCommand := &cobra.Command{
		Use:          "check",
		Short:        "Checks the status of async migrations for the store by checking the status of async migrations in the migration table.",
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
			check, err := newAsyncCheck(config.Clickhouse.DSN, config.Clickhouse.Cluster, config.Clickhouse.Replication, config.MigrateSyncCheck.Timeout, dbNames, logger)
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

	config.MigrateAsyncCheck.RegisterFlags(syncCheckCommand)

	parentCmd.AddCommand(syncCheckCommand)
}

func newAsyncCheck(dsn string, cluster string, replication bool, timeout time.Duration, dbNames schemamigrator.DatabaseNames, logger *zap.Logger) (*asyncCheck, error) {
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

	return &asyncCheck{
		conn:             conn,
		timeout:          timeout,
		migrationManager: migrationManager,
		dbNames:          dbNames,
		logger:           logger,
	}, nil
}

func (cmd *asyncCheck) Run(ctx context.Context) error {
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

func (cmd *asyncCheck) Check(ctx context.Context) error {
	tracesLastMigrationID, err := cmd.getLastAsyncMigration(schemamigrator.TracesMigrations)
	if err == nil {
		ok, err := cmd.migrationManager.CheckMigrationStatus(ctx, cmd.dbNames.Traces, tracesLastMigrationID, schemamigrator.FinishedStatus)
		if err != nil {
			return err
		}

		if !ok {
			return fmt.Errorf("migration with ID %d for database '%s' has not been completed", tracesLastMigrationID, cmd.dbNames.Traces)
		}
	}

	logsMigrations := schemamigrator.LogsMigrations
	if constants.EnableLogsMigrationsV2 {
		logsMigrations = schemamigrator.LogsMigrationsV2
	}

	logsLastMigrationID, err := cmd.getLastAsyncMigration(logsMigrations)
	if err == nil {
		ok, err := cmd.migrationManager.CheckMigrationStatus(ctx, cmd.dbNames.Logs, logsLastMigrationID, schemamigrator.FinishedStatus)
		if err != nil {
			return err
		}

		if !ok {
			return fmt.Errorf("migration with ID %d for database '%s' has not been completed", logsLastMigrationID, cmd.dbNames.Logs)
		}
	}

	metricsLastMigrationID, err := cmd.getLastAsyncMigration(schemamigrator.MetricsMigrations)
	if err == nil {
		ok, err := cmd.migrationManager.CheckMigrationStatus(ctx, cmd.dbNames.Metrics, metricsLastMigrationID, schemamigrator.FinishedStatus)
		if err != nil {
			return err
		}

		if !ok {
			return fmt.Errorf("migration with ID %d for database '%s' has not been completed", metricsLastMigrationID, cmd.dbNames.Metrics)
		}
	}

	metadataLastMigrationID, err := cmd.getLastAsyncMigration(schemamigrator.MetadataMigrations)
	if err == nil {
		ok, err := cmd.migrationManager.CheckMigrationStatus(ctx, cmd.dbNames.Metadata, metadataLastMigrationID, schemamigrator.FinishedStatus)
		if err != nil {
			return err
		}

		if !ok {
			return fmt.Errorf("migration with ID %d for database '%s' has not been completed", metadataLastMigrationID, cmd.dbNames.Metadata)
		}
		return err
	}

	analyticsLastMigrationID, err := cmd.getLastAsyncMigration(schemamigrator.AnalyticsMigrations)
	if err == nil {
		ok, err := cmd.migrationManager.CheckMigrationStatus(ctx, cmd.dbNames.Analytics, analyticsLastMigrationID, schemamigrator.FinishedStatus)
		if err != nil {
			return err
		}

		if !ok {
			return fmt.Errorf("migration with ID %d for database '%s' has not been completed", analyticsLastMigrationID, cmd.dbNames.Analytics)
		}
		return err
	}

	meterLastMigrationID, err := cmd.getLastAsyncMigration(schemamigrator.MeterMigrations)
	if err == nil {
		ok, err := cmd.migrationManager.CheckMigrationStatus(ctx, cmd.dbNames.Meter, meterLastMigrationID, schemamigrator.FinishedStatus)
		if err != nil {
			return err
		}

		if !ok {
			return fmt.Errorf("migration with ID %d for database '%s' has not been completed", meterLastMigrationID, cmd.dbNames.Meter)
		}
		return err
	}

	return nil
}

func (cmd *asyncCheck) getLastAsyncMigration(migrations []schemamigrator.SchemaMigrationRecord) (uint64, error) {
	for i := len(migrations) - 1; i >= 0; i-- {
		if cmd.migrationManager.IsAsync(migrations[i]) {
			return migrations[i].MigrationID, nil
		}
	}

	return 0, fmt.Errorf("no async migrations found")
}
