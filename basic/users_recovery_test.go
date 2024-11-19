package basic

import (
	"fmt"
	"github.com/Netcracker/dbaas-opensearch-adapter/common"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestUserContentWithResourcePrefix(t *testing.T) {
	username := "admin"
	password := common.GenerateUUID()
	resourcePrefix := common.GetUUID()
	roleType := AdminRoleType
	connectionProperties := common.ConnectionProperties{
		Username:       username,
		Password:       password,
		ResourcePrefix: resourcePrefix,
		Role:           roleType,
	}
	content := bp.getUserContent(connectionProperties)
	expectedAttributes := map[string]string{resourcePrefixAttributeName: resourcePrefix}
	expectedBackendRoles := bp.GetBackendRoles(roleType)
	assert.Equal(t, password, content.Password)
	assert.EqualValues(t, expectedAttributes, content.Attributes)
	assert.EqualValues(t, expectedBackendRoles, content.BackendRoles)
}

func TestUserContentWithoutResourcePrefix(t *testing.T) {
	username := "admin"
	password := common.GenerateUUID()
	roleType := AdminRoleType
	dbName := fmt.Sprintf("%s_test", common.GetUUID())
	connectionProperties := common.ConnectionProperties{
		DbName:   dbName,
		Username: username,
		Password: password,
		Role:     roleType,
	}
	content := bp.getUserContent(connectionProperties)
	expectedAttributes := map[string]string{resourcePrefixAttributeName: dbName}
	expectedBackendRoles := bp.GetBackendRoles(roleType)
	assert.Equal(t, password, content.Password)
	assert.EqualValues(t, expectedAttributes, content.Attributes)
	assert.EqualValues(t, expectedBackendRoles, content.BackendRoles)
}

func TestUserContentWithResourcePrefixAndDbName(t *testing.T) {
	username := "admin"
	password := common.GenerateUUID()
	resourcePrefix := common.GetUUID()
	roleType := AdminRoleType
	dbName := fmt.Sprintf("%s_test", resourcePrefix)
	connectionProperties := common.ConnectionProperties{
		DbName:         dbName,
		Username:       username,
		Password:       password,
		ResourcePrefix: resourcePrefix,
		Role:           roleType,
	}
	content := bp.getUserContent(connectionProperties)
	expectedAttributes := map[string]string{resourcePrefixAttributeName: resourcePrefix}
	expectedBackendRoles := bp.GetBackendRoles(roleType)
	assert.Equal(t, password, content.Password)
	assert.EqualValues(t, expectedAttributes, content.Attributes)
	assert.EqualValues(t, expectedBackendRoles, content.BackendRoles)
}

func TestUserContentWithReadOnlyRole(t *testing.T) {
	username := "admin"
	password := common.GenerateUUID()
	resourcePrefix := common.GetUUID()
	roleType := ReadOnlyRoleType
	connectionProperties := common.ConnectionProperties{
		Username:       username,
		Password:       password,
		ResourcePrefix: resourcePrefix,
		Role:           roleType,
	}
	content := bp.getUserContent(connectionProperties)
	expectedAttributes := map[string]string{resourcePrefixAttributeName: resourcePrefix}
	expectedBackendRoles := bp.GetBackendRoles(roleType)
	assert.Equal(t, password, content.Password)
	assert.EqualValues(t, expectedAttributes, content.Attributes)
	assert.EqualValues(t, expectedBackendRoles, content.BackendRoles)
}

func TestUserContentWithDmlRole(t *testing.T) {
	username := "admin"
	password := common.GenerateUUID()
	resourcePrefix := common.GetUUID()
	roleType := DmlRoleType
	connectionProperties := common.ConnectionProperties{
		Username:       username,
		Password:       password,
		ResourcePrefix: resourcePrefix,
		Role:           roleType,
	}
	content := bp.getUserContent(connectionProperties)
	expectedAttributes := map[string]string{resourcePrefixAttributeName: resourcePrefix}
	expectedBackendRoles := bp.GetBackendRoles(roleType)
	assert.Equal(t, password, content.Password)
	assert.EqualValues(t, expectedAttributes, content.Attributes)
	assert.EqualValues(t, expectedBackendRoles, content.BackendRoles)
}

func TestUserContentWithIsmRole(t *testing.T) {
	username := "admin"
	password := common.GenerateUUID()
	roleType := IsmRoleType
	connectionProperties := common.ConnectionProperties{
		Username: username,
		Password: password,
		Role:     roleType,
	}
	content := bp.getUserContent(connectionProperties)
	expectedBackendRoles := bp.GetBackendRoles(roleType)
	assert.Equal(t, password, content.Password)
	assert.EqualValues(t, expectedBackendRoles, content.BackendRoles)
}

func TestUserContentWithoutRole(t *testing.T) {
	username := "admin"
	password := common.GenerateUUID()
	resourcePrefix := common.GetUUID()
	connectionProperties := common.ConnectionProperties{
		Username:       username,
		Password:       password,
		ResourcePrefix: resourcePrefix,
	}
	content := bp.getUserContent(connectionProperties)
	expectedAttributes := map[string]string{resourcePrefixAttributeName: resourcePrefix}
	expectedBackendRoles := bp.GetBackendRoles(AdminRoleType)
	assert.Equal(t, password, content.Password)
	assert.EqualValues(t, expectedAttributes, content.Attributes)
	assert.EqualValues(t, expectedBackendRoles, content.BackendRoles)
}
