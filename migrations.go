package userintelligence

import (
	"io/fs"

	postgresmigrations "github.com/open-rails/user-intelligence/migrations/postgres"
)

const MigrationAppName = "user-intelligence"

var PostgresMigrationsFS fs.FS = postgresmigrations.FS
