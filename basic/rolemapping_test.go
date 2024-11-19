package basic

import (
	"fmt"
	"github.com/Netcracker/dbaas-opensearch-adapter/common"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetRoleMappingForAdminRole(t *testing.T) {
	name := fmt.Sprintf(common.RoleNamePattern, AdminRoleType)
	roleMapping, err := baseProvider.GetRoleMapping(name)
	assert.Empty(t, err)
	assert.False(t, roleMapping.Reserved)
	assert.Len(t, roleMapping.Users, 10)
}

func TestGetRolesMapping(t *testing.T) {
	rolesMapping, err := baseProvider.GetRolesMapping()
	assert.Empty(t, err)
	assert.Len(t, rolesMapping, 6)
}

func TestUpdateRoleMapping(t *testing.T) {
	err := baseProvider.CreateOrUpdateRoleMapping(AdminRoleType)
	assert.Empty(t, err)
}
