using System;
using Amazon.DynamoDBv2.Model;
using NUnit.Framework;
using Suplex.Security.AclModel;
using Suplex.Security.Principal;

namespace Suplex.Security.DynamoDbDal.Tests
{
    [TestFixture]
    public class DynamoDbDalTests
    {
        private string _userTable = "Suplex.User";
        private string _userPrefix = "User.";
        private string _groupTable = "Suplex.Group";
        private string _groupPrefix = "Group.";
        private string _groupMembershipTable = "Suplex.GroupMembership";
        private string _secureObjectTable = "Suplex.SecureObject";
        private string _secureObjectPrefix = "SecureObject.";

        [Test]
        public void UpsertUser_Valid_Succeeds()
        {
            User user = new User()
            {
                Name = _userPrefix,
                IsBuiltIn = true,
                IsEnabled = true,
                IsLocal = true
            };
            user.Name = user.Name + user.UId;

            DynamoDbDal dal = new DynamoDbDal
            {
                UserTable = _userTable
            };
            User upsertedUser = dal.UpsertUser( user );
            Assert.AreEqual( upsertedUser.UId, user.UId );
            Assert.AreEqual( upsertedUser.Name, user.Name );
        }

        [Test]
        public void UpsertUser_NonExistent_Table_Throws_Exception()
        {
            User user = new User()
            {
                Name = _userPrefix,
                IsBuiltIn = true,
                IsEnabled = true,
                IsLocal = true
            };
            user.Name = user.Name + user.UId;

            DynamoDbDal dal = new DynamoDbDal
            {
                UserTable = Guid.NewGuid().ToString()
            };

            ResourceNotFoundException ex = Assert.Throws<ResourceNotFoundException>( () => dal.UpsertUser( user ) );
            StringAssert.Contains( "Requested resource not found: Table", ex.Message );
        }


        [Test]
        public void GetUserByUId_Existing_User_Succeeds()
        {
            // Arrange
            User user = new User()
            {
                Name = _userPrefix,
                IsBuiltIn = true,
                IsEnabled = true,
                IsLocal = true
            };
            user.Name = user.Name + user.UId;

            DynamoDbDal dal = new DynamoDbDal
            {
                UserTable = _userTable
            };
            dal.UpsertUser( user );

            // Act
            User retUser = dal.GetUserByUId( user.UId.Value );

            // Assert
            Assert.AreEqual( retUser.UId, user.UId );
            Assert.AreEqual( retUser.Name, user.Name );
            Assert.AreEqual( retUser.IsEnabled, user.IsEnabled );
            Assert.AreEqual( retUser.IsLocal, user.IsLocal );
        }

        [Test]
        public void GetUserByUId_Empty_UserUId_Throws_Exception()
        {
            // Arrange
            Guid userUId = Guid.Empty;
            DynamoDbDal dal = new DynamoDbDal
            {
                UserTable = _userTable
            };

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.GetUserByUId( userUId ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( ex.Message, "User unique Id cannot be empty." );
        }

        [Test]
        public void GetUserByUId_Null_User_Table_Throws_Exception()
        {
            // Arrange
            Guid userUId = Guid.NewGuid();
            DynamoDbDal dal = new DynamoDbDal
            {
                UserTable = ""
            };

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.GetUserByUId( userUId ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( ex.Message, "User table name must be specified." );
        }

        [Test]
        public void GetUserByUId_Non_Existent_Table_Throws_Exception()
        {
            // Arrange
            Guid userUId = Guid.NewGuid();

            DynamoDbDal dal = new DynamoDbDal
            {
                UserTable = "Table-" + Guid.NewGuid()
            };

            // Act
            Exception ex = Assert.Throws<ResourceNotFoundException>( () => dal.GetUserByUId( userUId ) );

            // Assert
            StringAssert.Contains( "Requested resource not found", ex.Message );
        }

        [Test]
        public void GetUserByUId_Non_Existent_UserUId_Throws_Exception()
        {
            // Arrange
            Guid userUId = Guid.NewGuid();

            DynamoDbDal dal = new DynamoDbDal
            {
                UserTable = _userTable
            };

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.GetUserByUId( userUId ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( "User cannot be found.", ex.Message );
        }


        [Test]
        public void GetUserByName_Existing_User_Succeeds()
        {
            User newUser = new User()
            {
                Name = _userPrefix,
                IsBuiltIn = true,
                IsEnabled = true,
                IsLocal = true
            };
            newUser.Name = newUser.Name + newUser.UId;

            DynamoDbDal dal = new DynamoDbDal
            {
                UserTable = _userTable
            };
            dal.UpsertUser( newUser );

            var retrievedUsers = dal.GetUserByName( newUser.Name );

            foreach ( User user in retrievedUsers )
            {
                Assert.AreEqual( user.UId, user.UId );
                Assert.AreEqual( user.Name, user.Name );
            }
        }

        [Test]
        public void DeleteUser_Existing_User_Succeeds()
        {
            User newUser = new User()
            {
                Name = _userPrefix,
                IsBuiltIn = true,
                IsEnabled = true,
                IsLocal = true
            };
            newUser.Name = newUser.Name + newUser.UId;

            DynamoDbDal dal = new DynamoDbDal
            {
                UserTable = _userTable
            };
            dal.UpsertUser( newUser );

            Assert.DoesNotThrow( () => dal.DeleteUser( newUser.UId.Value ) );
        }

        [Test]
        public void UpsertGroup_Valid_Succeeds()
        {
            Group group = new Group()
            {
                Name = _groupPrefix,
                IsBuiltIn = false,
                IsEnabled = true,
                IsLocal = false
            };
            group.Name = group.Name + group.UId;

            DynamoDbDal dal = new DynamoDbDal
            {
                GroupTable = _groupTable
            };
            Group upsertedGroup = dal.UpsertGroup( group );
            Assert.AreEqual( upsertedGroup.UId, group.UId );
            Assert.AreEqual( upsertedGroup.Name, group.Name );
        }

        [Test]
        public void GetGroupByUId_Existing_Group_Succeeds()
        {
            Group group = new Group()
            {
                Name = _groupPrefix,
                IsBuiltIn = true,
                IsEnabled = true,
                IsLocal = true
            };
            group.Name = group.Name + group.UId;

            DynamoDbDal dal = new DynamoDbDal
            {
                GroupTable = _groupTable
            };
            dal.UpsertGroup( group );

            Group retrievedGroup = dal.GetGroupByUId( group.UId.Value );
            Assert.AreEqual( retrievedGroup.UId, group.UId );
            Assert.AreEqual( retrievedGroup.Name, group.Name );
        }

        [Test]
        public void DeleteGroup_Existing_Group_Succeeds()
        {
            Group newGroup = new Group()
            {
                Name = _groupPrefix,
                IsBuiltIn = true,
                IsEnabled = true,
                IsLocal = true
            };
            newGroup.Name = newGroup.Name + newGroup.UId;

            DynamoDbDal dal = new DynamoDbDal
            {
                GroupTable = _groupTable
            };
            dal.UpsertGroup( newGroup );

            Assert.DoesNotThrow( () => dal.DeleteGroup( newGroup.UId.Value ) );
        }

        [Test]
        public void GetGroupByName_Existing_Group_Succeeds()
        {
            Group newGroup = new Group()
            {
                Name = _groupPrefix,
                IsBuiltIn = true,
                IsEnabled = true,
                IsLocal = true
            };
            newGroup.Name = newGroup.Name + newGroup.UId;

            DynamoDbDal dal = new DynamoDbDal
            {
                GroupTable = _groupTable
            };
            dal.UpsertGroup( newGroup );

            var retrievedGroups = dal.GetGroupByName( newGroup.Name );

            foreach ( Group group in retrievedGroups )
            {
                Assert.AreEqual( group.UId, group.UId );
                Assert.AreEqual( group.Name, group.Name );
            }
        }

        [Test]
        public void UpsertGroupMembership_Valid_Succeeds()
        {
            Guid groupUId = Guid.NewGuid();
            Guid memberUId = Guid.NewGuid();

            GroupMembershipItem groupMembershipItem = new GroupMembershipItem()
            {
                GroupUId = groupUId,
                MemberUId = memberUId,
                IsMemberUser = true
            };

            DynamoDbDal dal = new DynamoDbDal
            {
                GroupMembershipTable = _groupMembershipTable
            };
            GroupMembershipItem upsertedGroupMembershipItem = dal.UpsertGroupMembership( groupMembershipItem );
            Assert.AreEqual( upsertedGroupMembershipItem.GroupUId, groupMembershipItem.GroupUId );
            Assert.AreEqual( upsertedGroupMembershipItem.MemberUId, groupMembershipItem.MemberUId );
        }

        [Test]
        public void DeleteGroupMembership_Existing_Group_Succeeds()
        {
            Guid groupUId = Guid.NewGuid();
            Guid memberUId = Guid.NewGuid();

            GroupMembershipItem gmi = new GroupMembershipItem
            {
                GroupUId = groupUId,
                MemberUId = memberUId,
                IsMemberUser = true
            };

            DynamoDbDal dal = new DynamoDbDal
            {
                GroupMembershipTable = _groupMembershipTable
            };
            dal.UpsertGroupMembership( gmi );

            Assert.DoesNotThrow( () => dal.DeleteGroupMembership( gmi ) );
        }

        [Test]
        public void UpsertSecureObject_Valid_Succeeds()
        {
            SecureObject sObject = new SecureObject() { UniqueName = _secureObjectPrefix + Guid.NewGuid() };
            DiscretionaryAcl topdacl = new DiscretionaryAcl
                    {
                        new AccessControlEntry<FileSystemRight> { Allowed = true, Right = FileSystemRight.FullControl },
                        new AccessControlEntry<FileSystemRight> { Allowed = false, Right = FileSystemRight.Execute | FileSystemRight.List, Inheritable = false },
                        new AccessControlEntry<UIRight> { Right= UIRight.Operate | UIRight.Visible }
                    };
            sObject.Security.Dacl = topdacl;

            DynamoDbDal dal = new DynamoDbDal
            {
                SecureObjectTable = _secureObjectTable
            };
            ISecureObject secureObject = dal.UpsertSecureObject( sObject );
            Assert.AreEqual( secureObject.UId, sObject.UId );
            Assert.AreEqual( secureObject.UniqueName, sObject.UniqueName );
        }

        [Test]
        public void DeleteSecureObject_Existing_Succeeds()
        {
            SecureObject sObject = new SecureObject() { UniqueName = _secureObjectPrefix + Guid.NewGuid() };
            DiscretionaryAcl topdacl = new DiscretionaryAcl
                    {
                        new AccessControlEntry<FileSystemRight> { Allowed = true, Right = FileSystemRight.FullControl },
                        new AccessControlEntry<FileSystemRight> { Allowed = false, Right = FileSystemRight.Execute | FileSystemRight.List, Inheritable = false },
                        new AccessControlEntry<UIRight> { Right= UIRight.Operate | UIRight.Visible }
                    };
            sObject.Security.Dacl = topdacl;

            DynamoDbDal dal = new DynamoDbDal
            {
                SecureObjectTable = _secureObjectTable
            };
            dal.UpsertSecureObject( sObject );

            Assert.DoesNotThrow( () => dal.DeleteSecureObject( sObject.UId.Value ) );
        }

        [Test]
        public void GetSecureObjectByUId_Existing_Succeeds()
        {
            SecureObject sObject = new SecureObject() { UniqueName = _secureObjectPrefix + Guid.NewGuid() };
            DiscretionaryAcl topdacl = new DiscretionaryAcl
                    {
                        new AccessControlEntry<FileSystemRight> { Allowed = true, Right = FileSystemRight.FullControl },
                        new AccessControlEntry<FileSystemRight> { Allowed = false, Right = FileSystemRight.Execute | FileSystemRight.List, Inheritable = false },
                        new AccessControlEntry<UIRight> { Right= UIRight.Operate | UIRight.Visible }
                    };
            sObject.Security.Dacl = topdacl;

            DynamoDbDal dal = new DynamoDbDal
            {
                SecureObjectTable = _secureObjectTable
            };
            dal.UpsertSecureObject( sObject );
            ISecureObject secureObject = dal.GetSecureObjectByUId( sObject.UId.Value, false );
            Assert.AreEqual( secureObject.UId, sObject.UId );
            Assert.AreEqual( secureObject.UniqueName, sObject.UniqueName );
        }


        [Test]
        public void GetSecureObjectByUniqueName_Existing_Succeeds()
        {
            SecureObject sObject = new SecureObject() { UniqueName = _secureObjectPrefix + Guid.NewGuid() };
            DiscretionaryAcl topdacl = new DiscretionaryAcl
                    {
                        new AccessControlEntry<FileSystemRight> { Allowed = true, Right = FileSystemRight.FullControl },
                        new AccessControlEntry<FileSystemRight> { Allowed = false, Right = FileSystemRight.Execute | FileSystemRight.List, Inheritable = false },
                        new AccessControlEntry<UIRight> { Right= UIRight.Operate | UIRight.Visible }
                    };
            sObject.Security.Dacl = topdacl;

            DynamoDbDal dal = new DynamoDbDal
            {
                SecureObjectTable = _secureObjectTable
            };
            dal.UpsertSecureObject( sObject );
            ISecureObject secureObject = dal.GetSecureObjectByUniqueName( sObject.UniqueName, false );
            Assert.AreEqual( secureObject.UId, sObject.UId );
            Assert.AreEqual( secureObject.UniqueName, sObject.UniqueName );
        }

        [Test]
        public void GetGroupMembers_Existing_Group_Succeeds()
        {
            Guid groupUId = Guid.NewGuid();
            Guid memberUId = Guid.NewGuid();

            GroupMembershipItem groupMembershipItem = new GroupMembershipItem()
            {
                GroupUId = groupUId,
                MemberUId = memberUId,
                IsMemberUser = true
            };

            DynamoDbDal dal = new DynamoDbDal
            {
                GroupMembershipTable = _groupMembershipTable
            };
            dal.UpsertGroupMembership( groupMembershipItem );

            var retrievedGroups = dal.GetGroupMembers( groupUId );

            foreach ( var gmi in retrievedGroups )
            {
                Assert.AreEqual( gmi.GroupUId, groupUId );
            }
        }

        [Test]
        public void GetGroupMembership_Existing_Group_Succeeds()
        {
            Guid groupUId = Guid.NewGuid();
            Guid memberUId = Guid.NewGuid();

            GroupMembershipItem groupMembershipItem = new GroupMembershipItem()
            {
                GroupUId = groupUId,
                MemberUId = memberUId,
                IsMemberUser = true
            };

            DynamoDbDal dal = new DynamoDbDal
            {
                GroupMembershipTable = _groupMembershipTable
            };
            dal.UpsertGroupMembership( groupMembershipItem );

            var retrievedGroups = dal.GetGroupMembership( memberUId );

            foreach ( var gmi in retrievedGroups )
            {
                Assert.AreEqual( gmi.MemberUId, memberUId );
            }
        }
    }
}
