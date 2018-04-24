using System;
using System.Runtime.ExceptionServices;
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
        public void UpsertUser_Null_User_Throws_Exception()
        {
            // Arrange
            User user = null;

            DynamoDbDal dal = new DynamoDbDal
            {
                UserTable = _userTable
            };

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.UpsertUser( user ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( "User cannot be null.", ex.Message );

        }

        [Test]
        public void UpsertUser_Null_User_Table_Throws_Exception()
        {
            // Arrange
            User user = new User();

            DynamoDbDal dal = new DynamoDbDal
            {
                UserTable = ""
            };

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.UpsertUser( user ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( "User table name must be specified.", ex.Message );

        }

        [Test]
        public void UpsertUser_Valid_Details_Succeeds()
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

            // Act
            User upsertedUser = dal.UpsertUser( user );

            // Assert
            Assert.AreEqual( upsertedUser.UId, user.UId );
            Assert.AreEqual( upsertedUser.Name, user.Name );
        }

        [Test]
        public void UpsertUser_Existing_User_Succeeds()
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

            // Act
            dal.UpsertUser( user );
            user.IsEnabled = false;
            dal.UpsertUser( user );
            User retUser = dal.GetUserByUId( user.UId.Value );

            // Assert
            Assert.AreEqual( retUser.UId, user.UId );
            Assert.AreEqual( retUser.Name, user.Name );
            Assert.AreEqual( retUser.IsEnabled, false );
        }

        [Test]
        public void UpsertUser_Non_Existent_Table_Throws_Exception()
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
            StringAssert.AreEqualIgnoringCase( "User unique Id cannot be empty.", ex.Message );
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
            StringAssert.Contains( "Requested resource not found: Table", ex.Message );
        }

        [Test]
        public void GetUserByUId_Non_Existent_User_Throws_Exception()
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
        public void GetUserByName_Null_Empty_Name_Throws_Exception()
        {
            // Arrange
            string name = "";
            DynamoDbDal dal = new DynamoDbDal
            {
                UserTable = _userTable
            };

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.GetUserByName( name ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( "User's name must be specified.", ex.Message );

        }

        [Test]
        public void GetUserByName_Null_User_Table_Throws_Exception()
        {
            // Arrange
            string name = "XXX";
            DynamoDbDal dal = new DynamoDbDal
            {
                UserTable = ""
            };

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.GetUserByName( name ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( ex.Message, "User table name must be specified." );
        }

        [Test]
        public void GetUserByName_Non_Existent_User_Throws_Exception()
        {
            // Arrange
            string name = "XXX";

            DynamoDbDal dal = new DynamoDbDal
            {
                UserTable = _userTable
            };

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.GetUserByName( name ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( "User cannot be found.", ex.Message );
        }
        [Test]
        public void GetUserByName_Non_Existent_Table_Throws_Exception()
        {
            // Arrange
            string name = "XXX";

            DynamoDbDal dal = new DynamoDbDal
            {
                UserTable = "Table-" + Guid.NewGuid()
            };

            // Act
            Exception ex = Assert.Throws<ResourceNotFoundException>( () => dal.GetUserByName( name ) );

            // Assert
            StringAssert.Contains( "Requested resource not found: Table", ex.Message );
        }

        [Test]
        public void GetUserByName_Existing_User_Succeeds()
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
            dal.UpsertUser( user );

            var retUsers = dal.GetUserByName( user.Name );

            foreach ( User retUser in retUsers )
            {
                Assert.AreEqual( retUser.UId, user.UId );
                Assert.AreEqual( retUser.Name, user.Name );
                Assert.AreEqual( retUser.IsEnabled, user.IsEnabled );
                Assert.AreEqual( retUser.IsLocal, user.IsLocal );

            }
        }

        [Test]
        public void DeleteUser_Empty_UserUId_Throws_Exception()
        {
            // Arrange
            Guid userUId = Guid.Empty;
            DynamoDbDal dal = new DynamoDbDal
            {
                UserTable = _userTable
            };

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.DeleteUser( userUId ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( "User unique Id cannot be empty.", ex.Message );
        }

        [Test]
        public void DeleteUser_Null_User_Table_Throws_Exception()
        {
            // Arrange
            Guid userUId = Guid.NewGuid();
            DynamoDbDal dal = new DynamoDbDal
            {
                UserTable = ""
            };

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.DeleteUser( userUId ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( ex.Message, "User table name must be specified." );
        }

        [Test]
        public void DeleteUser_Non_Existent_Table_Throws_Exception()
        {
            // Arrange
            Guid userUId = Guid.NewGuid();

            DynamoDbDal dal = new DynamoDbDal
            {
                UserTable = "Table-" + Guid.NewGuid()
            };

            // Act
            Exception ex = Assert.Throws<ResourceNotFoundException>( () => dal.DeleteUser( userUId ) );

            // Assert
            StringAssert.Contains( "Requested resource not found: Table", ex.Message );
        }


        [Test]
        public void DeleteUser_Non_Existent_User_Succeeds()
        {
            // Arrange
            Guid userUId = Guid.NewGuid();

            DynamoDbDal dal = new DynamoDbDal
            {
                UserTable = _userTable
            };

            // Act
            // Assert
            Assert.DoesNotThrow( () => dal.DeleteUser( userUId ) );
        }

        [Test]
        public void DeleteUser_Existing_User_Succeeds()
        {
            // Arrange
            User newUser = new User()
            {
                Name = _userPrefix,
                IsBuiltIn = true,
                IsEnabled = true,
                IsLocal = true
            };
            newUser.Name = newUser.Name + newUser.UId;

            // Act
            DynamoDbDal dal = new DynamoDbDal
            {
                UserTable = _userTable
            };
            dal.UpsertUser( newUser );

            // Assert
            Assert.DoesNotThrow( () => dal.DeleteUser( newUser.UId.Value ) );
        }

        [Test]
        public void UpsertGroup_Null_Group_Throws_Exception()
        {
            // Arrange
            Group group = null;

            DynamoDbDal dal = new DynamoDbDal
            {
                GroupTable = _groupTable
            };

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.UpsertGroup( group ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( "Group cannot be null.", ex.Message );

        }

        [Test]
        public void UpsertGroup_Null_Group_Table_Throws_Exception()
        {
            // Arrange
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
                GroupTable = ""
            };

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.UpsertGroup( group ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( "Group table name must be specified.", ex.Message );

        }

        [Test]
        public void UpsertGroup_Valid_Details_Succeeds()
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
            Assert.AreEqual( group.UId, upsertedGroup.UId );
            Assert.AreEqual( group.Name, upsertedGroup.Name );
            Assert.AreEqual( group.IsEnabled, upsertedGroup.IsEnabled );
        }

        [Test]
        public void UpsertGroup_Non_Existent_Table_Throws_Exception()
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
                GroupTable = Guid.NewGuid().ToString()
            };

            ResourceNotFoundException ex = Assert.Throws<ResourceNotFoundException>( () => dal.UpsertGroup( group ) );
            StringAssert.Contains( "Requested resource not found: Table", ex.Message );
        }

        [Test]
        public void GetGroupByUId_Empty_GroupUId_Throws_Exception()
        {
            // Arrange
            Guid groupUId = Guid.Empty;
            DynamoDbDal dal = new DynamoDbDal
            {
                GroupTable = _groupTable
            };

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.GetGroupByUId( groupUId ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( "Group unique Id cannot be empty.", ex.Message );
        }

        [Test]
        public void GetGroupByUId_Null_Group_Table_Throws_Exception()
        {
            // Arrange
            Guid groupUId = Guid.NewGuid();
            DynamoDbDal dal = new DynamoDbDal
            {
                GroupTable = ""
            };

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.GetGroupByUId( groupUId ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( ex.Message, "Group table name must be specified." );
        }

        [Test]
        public void GetGroupByUId_Non_Existent_Table_Throws_Exception()
        {
            // Arrange
            Guid groupUId = Guid.NewGuid();

            DynamoDbDal dal = new DynamoDbDal
            {
                GroupTable = "Table-" + Guid.NewGuid()
            };

            // Act
            Exception ex = Assert.Throws<ResourceNotFoundException>( () => dal.GetGroupByUId( groupUId ) );

            // Assert
            StringAssert.Contains( "Requested resource not found: Table", ex.Message );
        }

        [Test]
        public void GetGroupByUId_Non_Existent_Group_Throws_Exception()
        {
            // Arrange
            Guid groupUId = Guid.NewGuid();

            DynamoDbDal dal = new DynamoDbDal
            {
                GroupTable = _groupTable
            };

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.GetGroupByUId( groupUId ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( "Group cannot be found.", ex.Message );
        }


        [Test]
        public void GetGroupByUId_Existing_Group_Succeeds()
        {
            Group group = new Group()
            {
                Name = _groupPrefix,
                IsBuiltIn = true,
                IsEnabled = true,
                IsLocal = false
            };
            group.Name = group.Name + group.UId;

            DynamoDbDal dal = new DynamoDbDal
            {
                GroupTable = _groupTable
            };
            dal.UpsertGroup( group );

            Group retGroup = dal.GetGroupByUId( group.UId.Value );
            Assert.AreEqual( retGroup.UId, group.UId );
            Assert.AreEqual( retGroup.Name, group.Name );
            Assert.AreEqual( retGroup.IsEnabled, group.IsEnabled );
        }

        [Test]
        public void DeleteGroup_Empty_GroupUId_Throws_Exception()
        {
            // Arrange
            Guid groupUId = Guid.Empty;
            DynamoDbDal dal = new DynamoDbDal
            {
                GroupTable = _groupTable
            };

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.DeleteGroup( groupUId ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( "Group unique Id cannot be empty.", ex.Message );
        }

        [Test]
        public void DeleteGroup_Null_Group_Table_Throws_Exception()
        {
            // Arrange
            Guid groupUId = Guid.NewGuid();
            DynamoDbDal dal = new DynamoDbDal
            {
                GroupTable = ""
            };

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.DeleteGroup( groupUId ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( ex.Message, "Group table name must be specified." );
        }

        [Test]
        public void DeleteGroup_Non_Existent_Table_Throws_Exception()
        {
            // Arrange
            Guid groupUId = Guid.NewGuid();

            DynamoDbDal dal = new DynamoDbDal
            {
                GroupTable = "Table-" + Guid.NewGuid()
            };

            // Act
            Exception ex = Assert.Throws<ResourceNotFoundException>( () => dal.DeleteGroup( groupUId ) );

            // Assert
            StringAssert.Contains( "Requested resource not found: Table", ex.Message );
        }


        [Test]
        public void DeleteGroup_Non_Existent_Group_Succeeds()
        {
            // Arrange
            Guid groupUId = Guid.NewGuid();

            DynamoDbDal dal = new DynamoDbDal
            {
                GroupTable = _groupTable
            };

            // Act
            // Assert
            Assert.DoesNotThrow( () => dal.DeleteGroup( groupUId ) );
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
        public void GetGroupByName_Null_Empty_Name_Throws_Exception()
        {
            // Arrange
            string name = "";
            DynamoDbDal dal = new DynamoDbDal
            {
                GroupTable = _groupTable
            };

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.GetGroupByName( name ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( "Group's name must be specified.", ex.Message );

        }

        [Test]
        public void GetGroupByName_Null_Group_Table_Throws_Exception()
        {
            // Arrange
            string name = "XXX";
            DynamoDbDal dal = new DynamoDbDal
            {
                GroupTable = ""
            };

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.GetGroupByName( name ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( ex.Message, "Group table name must be specified." );
        }

        [Test]
        public void GetGroupByName_Non_Existent_Group_Throws_Exception()
        {
            // Arrange
            string name = "XXX";

            DynamoDbDal dal = new DynamoDbDal
            {
                GroupTable = _groupTable
            };

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.GetGroupByName( name ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( "Group cannot be found.", ex.Message );
        }
        [Test]
        public void GetGroupByName_Non_Existent_Table_Throws_Exception()
        {
            // Arrange
            string name = "XXX";

            DynamoDbDal dal = new DynamoDbDal
            {
                GroupTable = "Table-" + Guid.NewGuid()
            };

            // Act
            Exception ex = Assert.Throws<ResourceNotFoundException>( () => dal.GetGroupByName( name ) );

            // Assert
            StringAssert.Contains( "Requested resource not found: Table", ex.Message );
        }


        [Test]
        public void GetGroupByName_Existing_Group_Succeeds()
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

            var retGroups = dal.GetGroupByName( group.Name );

            foreach ( Group retGroup in retGroups )
            {
                Assert.AreEqual( retGroup.UId, group.UId );
                Assert.AreEqual( retGroup.Name, group.Name );
                Assert.AreEqual( retGroup.IsEnabled, group.IsEnabled );
            }
        }

        [Test]
        public void UpsertGroupMembership_Null_Group_Membership_Throws_Exception()
        {
            // Arrange
            DynamoDbDal dal = new DynamoDbDal
            {
                GroupMembershipTable = _groupMembershipTable
            };

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.UpsertGroupMembership( null ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( "Group membership cannot be null.", ex.Message );
        }

        [Test]
        public void UpsertGroupMembership_Null_Group_Throws_Exception()
        {
            // Arrange
            Guid groupUId = Guid.Empty;
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

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.UpsertGroupMembership( groupMembershipItem ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( "Group unique Id cannot be empty.", ex.Message );
        }

        [Test]
        public void UpsertGroupMembership_Null_Group_Membership_Table_Throws_Exception()
        {
            // Arrange
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
                GroupMembershipTable = ""
            };

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.UpsertGroupMembership( groupMembershipItem ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( "Group membership table name must be specified.", ex.Message );
        }


        [Test]
        public void UpsertGroupMembership_Non_Existent_Table_Throws_Exception()
        {
            // Arrange
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
                GroupMembershipTable = "Table-" + Guid.NewGuid()
            };

            ResourceNotFoundException ex = Assert.Throws<ResourceNotFoundException>( () => dal.UpsertGroupMembership( groupMembershipItem ) );
            StringAssert.Contains( "Requested resource not found: Table", ex.Message );
        }

        [Test]
        public void UpsertGroupMembership_Valid_Details_Succeeds()
        {
            // Arrange
            Group group = new Group()
            {
                Name = _groupPrefix,
                IsBuiltIn = false,
                IsEnabled = true,
                IsLocal = false
            };
            group.Name = group.Name + group.UId;

            User user = new User()
            {
                Name = _userPrefix,
                IsBuiltIn = true,
                IsEnabled = false,
                IsLocal = true
            };
            user.Name = user.Name + user.UId;


            GroupMembershipItem groupMembershipItem = new GroupMembershipItem()
            {
                Group = group,
                Member = user,
                GroupUId = group.UId.Value,
                MemberUId = user.UId.Value,
                IsMemberUser = true
            };

            DynamoDbDal dal = new DynamoDbDal
            {
                GroupMembershipTable = _groupMembershipTable
            };

            // Act
            dal.UpsertGroupMembership( groupMembershipItem );
            var retMembership = dal.GetGroupMembership( user.UId.Value, true );

            // Assert
            foreach ( GroupMembershipItem gmi in retMembership )
            {
                Assert.AreEqual( group.UId, gmi.GroupUId );
                Assert.AreEqual( user.UId, gmi.MemberUId );
            }
        }

        [Test]
        public void UpsertGroupMembership_Existing_Group_Membership_Succeeds()
        {
            // Arrange
            Group group = new Group()
            {
                Name = _groupPrefix,
                IsBuiltIn = false,
                IsEnabled = true,
                IsLocal = false
            };
            group.Name = group.Name + group.UId;

            User user = new User()
            {
                Name = _userPrefix,
                IsBuiltIn = true,
                IsEnabled = false,
                IsLocal = true
            };
            user.Name = user.Name + user.UId;


            GroupMembershipItem groupMembershipItem = new GroupMembershipItem()
            {
                Group = group,
                Member = user,
                GroupUId = group.UId.Value,
                MemberUId = user.UId.Value,
                IsMemberUser = true
            };

            DynamoDbDal dal = new DynamoDbDal
            {
                GroupMembershipTable = _groupMembershipTable
            };

            // Act
            dal.UpsertGroupMembership( groupMembershipItem );
            groupMembershipItem.IsMemberUser = false;
            dal.UpsertGroupMembership( groupMembershipItem );
            var retMembership = dal.GetGroupMembership( user.UId.Value, true );

            // Assert
            foreach ( GroupMembershipItem gmi in retMembership )
            {
                Assert.AreEqual( groupMembershipItem.GroupUId, gmi.GroupUId );
                Assert.AreEqual( groupMembershipItem.MemberUId, gmi.MemberUId );
                Assert.AreEqual( groupMembershipItem.IsMemberUser, gmi.IsMemberUser );
            }
        }

        [Test]
        public void DeleteGroupMembership_Null_Group_Membership_Throws_Exception()
        {
            // Arrange
            DynamoDbDal dal = new DynamoDbDal
            {
                GroupMembershipTable = _groupMembershipTable
            };

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.DeleteGroupMembership( null ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( "Group membership cannot be null.", ex.Message );
        }

        [Test]
        public void DeleteGroupMembership_Empty_Group_Throws_Exception()
        {
            // Arrange
            Guid groupUId = Guid.Empty;
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

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.DeleteGroupMembership( gmi ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( ex.Message, "Group unique Id cannot be empty." );
        }

        [Test]
        public void DeleteGroupMembership_Empty_Member_Throws_Exception()
        {
            // Arrange
            Guid groupUId = Guid.NewGuid();
            Guid memberUId = Guid.Empty;
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

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.DeleteGroupMembership( gmi ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( ex.Message, "Member unique Id cannot be empty." );
        }

        [Test]
        public void DeleteGroupMembership_Null_Group_Membership_Table_Throws_Exception()
        {
            // Arrange
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
                GroupMembershipTable = ""
            };

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.DeleteGroupMembership( gmi ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( "Group membership table name must be specified.", ex.Message );
        }

        [Test]
        public void DeleteGroupMembership_Non_Existent_Table_Throws_Exception()
        {
            // Arrange
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
                GroupMembershipTable = "Table-" + Guid.NewGuid()
            };

            // Act
            Exception ex = Assert.Throws<ResourceNotFoundException>( () => dal.DeleteGroupMembership( gmi ) );

            // Assert
            StringAssert.Contains( "Requested resource not found: Table", ex.Message );
        }


        [Test]
        public void DeleteGroupMembership_Non_Existent_Group_Membership_Succeeds()
        {
            // Arrange
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
                GroupMembershipTable = "Table-" + Guid.NewGuid()
            };

            // Act
            // Assert
            Assert.DoesNotThrow( () => dal.DeleteGroupMembership( gmi ) );
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
        public void GetGroupMembers_Empty_GroupUId_Throws_Exception()
        {
            // Arrange
            Guid groupUId = Guid.Empty;

            DynamoDbDal dal = new DynamoDbDal
            {
                GroupMembershipTable = _groupMembershipTable
            };

            // Act
            // Assert
            Exception ex = Assert.Throws<Exception>( () => dal.GetGroupMembers( groupUId ) );
            StringAssert.AreEqualIgnoringCase( "Group unique Id cannot be empty.", ex.Message );
        }


        [Test]
        public void GetGroupMembers_Null_Group_Membership_Table_Throws_Exception()
        {
            // Arrange
            Guid groupUId = Guid.NewGuid();

            DynamoDbDal dal = new DynamoDbDal
            {
                GroupMembershipTable = ""
            };

            // Act
            // Assert
            Exception ex = Assert.Throws<Exception>( () => dal.GetGroupMembers( groupUId ) );
            StringAssert.AreEqualIgnoringCase( "Group membership table name must be specified.", ex.Message );
        }

        [Test]
        public void GetGroupMembers_Non_Existent_Table_Throws_Exception()
        {
            // Arrange
            Guid groupUId = Guid.NewGuid();

            DynamoDbDal dal = new DynamoDbDal
            {
                GroupMembershipTable = "Table-" + Guid.NewGuid()
            };

            // Act
            // Assert
            ResourceNotFoundException ex = Assert.Throws<ResourceNotFoundException>( () => dal.GetGroupMembers( groupUId ) );
            StringAssert.Contains( "Requested resource not found: Table", ex.Message );
        }


        [Test]
        public void GetGroupMembers_Non_Existent_Group_Throws_Exception()
        {
            // Arrange
            Guid groupUId = Guid.NewGuid();

            DynamoDbDal dal = new DynamoDbDal
            {
                GroupMembershipTable = _groupMembershipTable
            };

            // Act
            // Assert
            Exception ex = Assert.Throws<Exception>( () => dal.GetGroupMembers( groupUId ) );
            StringAssert.Contains( "Group members cannot be found.", ex.Message );
        }

        [Test]
        public void GetGroupMembers_Existing_Group_Include_Disabled_Membership_Succeeds()
        {
            // Arrange
            Group group = new Group()
            {
                Name = _groupPrefix,
                IsBuiltIn = false,
                IsEnabled = true,
                IsLocal = false
            };
            group.Name = group.Name + group.UId;

            User user = new User()
            {
                Name = _userPrefix,
                IsBuiltIn = true,
                IsEnabled = false,
                IsLocal = true
            };
            user.Name = user.Name + user.UId;


            GroupMembershipItem groupMembershipItem = new GroupMembershipItem()
            {
                Group = group,
                Member = user,
                GroupUId = group.UId.Value,
                MemberUId = user.UId.Value,
                IsMemberUser = true
            };

            DynamoDbDal dal = new DynamoDbDal
            {
                GroupMembershipTable = _groupMembershipTable
            };

            // Act
            dal.UpsertGroupMembership( groupMembershipItem );
            var retMembership = dal.GetGroupMembers( group.UId.Value, true );

            // Assert
            foreach ( GroupMembershipItem gmi in retMembership )
            {
                Assert.AreEqual( group.UId, gmi.GroupUId );
                Assert.AreEqual( user.UId, gmi.MemberUId );
            }
        }

        [Test]
        public void GetGroupMembers_Existing_Group_Exclude_Disabled_Membership_Succeeds()
        {
            // Arrange
            Group group = new Group()
            {
                Name = _groupPrefix,
                IsBuiltIn = false,
                IsEnabled = true,
                IsLocal = false
            };
            group.Name = group.Name + group.UId;

            User user = new User()
            {
                Name = _userPrefix,
                IsBuiltIn = true,
                IsEnabled = false,
                IsLocal = true
            };
            user.Name = user.Name + user.UId;


            GroupMembershipItem groupMembershipItem = new GroupMembershipItem()
            {
                Group = group,
                Member = user,
                GroupUId = group.UId.Value,
                MemberUId = user.UId.Value,
                IsMemberUser = true
            };

            DynamoDbDal dal = new DynamoDbDal
            {
                GroupMembershipTable = _groupMembershipTable
            };

            // Act
            dal.UpsertGroupMembership( groupMembershipItem );
            var retMembership = dal.GetGroupMembers( group.UId.Value, false );
            int count = 0;
            using ( var enumerator = retMembership.GetEnumerator() )
            {
                while ( enumerator.MoveNext() )
                    count++;
            }

            // Assert
            Assert.AreEqual( 0, count );
        }

        [Test]
        public void GetGroupMembers_Empty_MemberUId_Throws_Exception()
        {
            // Arrange
            Guid groupUId = Guid.Empty;

            DynamoDbDal dal = new DynamoDbDal
            {
                GroupMembershipTable = _groupMembershipTable
            };

            // Act
            // Assert
            Exception ex = Assert.Throws<Exception>( () => dal.GetGroupMembers( groupUId ) );
            StringAssert.AreEqualIgnoringCase( "Group unique Id cannot be empty.", ex.Message );
        }


        [Test]
        public void GetGroupMembership_Null_Group_Membership_Table_Throws_Exception()
        {
            // Arrange
            Guid memberUId = Guid.NewGuid();

            DynamoDbDal dal = new DynamoDbDal
            {
                GroupMembershipTable = ""
            };

            // Act
            // Assert
            Exception ex = Assert.Throws<Exception>( () => dal.GetGroupMembership( memberUId ) );
            StringAssert.AreEqualIgnoringCase( "Group membership table name must be specified.", ex.Message );
        }

        [Test]
        public void GetGroupMembership_Non_Existent_Table_Throws_Exception()
        {
            // Arrange
            Guid memberUId = Guid.NewGuid();

            DynamoDbDal dal = new DynamoDbDal
            {
                GroupMembershipTable = "Table-" + Guid.NewGuid()
            };

            // Act
            // Assert
            ResourceNotFoundException ex = Assert.Throws<ResourceNotFoundException>( () => dal.GetGroupMembership( memberUId ) );
            StringAssert.Contains( "Requested resource not found: Table", ex.Message );
        }


        [Test]
        public void GetGroupMembership_Non_Existent_Member_Throws_Exception()
        {
            // Arrange
            Guid memberUId = Guid.NewGuid();

            DynamoDbDal dal = new DynamoDbDal
            {
                GroupMembershipTable = _groupMembershipTable
            };

            // Act
            // Assert
            Exception ex = Assert.Throws<Exception>( () => dal.GetGroupMembership( memberUId ) );
            StringAssert.Contains( "Group members cannot be found.", ex.Message );
        }

        [Test]
        public void GetGroupMembership_Existing_Member_Include_Disabled_Membership_Succeeds()
        {
            // Arrange
            Group group = new Group()
            {
                Name = _groupPrefix,
                IsBuiltIn = false,
                IsEnabled = true,
                IsLocal = false
            };
            group.Name = group.Name + group.UId;

            User user = new User()
            {
                Name = _userPrefix,
                IsBuiltIn = true,
                IsEnabled = false,
                IsLocal = true
            };
            user.Name = user.Name + user.UId;


            GroupMembershipItem groupMembershipItem = new GroupMembershipItem()
            {
                Group = group,
                Member = user,
                GroupUId = group.UId.Value,
                MemberUId = user.UId.Value,
                IsMemberUser = true
            };

            DynamoDbDal dal = new DynamoDbDal
            {
                GroupMembershipTable = _groupMembershipTable
            };

            // Act
            dal.UpsertGroupMembership( groupMembershipItem );
            var retMembership = dal.GetGroupMembership( user.UId.Value, true );

            // Assert
            foreach ( GroupMembershipItem gmi in retMembership )
            {
                Assert.AreEqual( group.UId, gmi.GroupUId );
                Assert.AreEqual( user.UId, gmi.MemberUId );
            }
        }

        [Test]
        public void GetGroupMembership_Existing_Member_Exclude_Disabled_Membership_Succeeds()
        {
            // Arrange
            Group group = new Group()
            {
                Name = _groupPrefix,
                IsBuiltIn = false,
                IsEnabled = true,
                IsLocal = false
            };
            group.Name = group.Name + group.UId;

            User user = new User()
            {
                Name = _userPrefix,
                IsBuiltIn = true,
                IsEnabled = false,
                IsLocal = true
            };
            user.Name = user.Name + user.UId;


            GroupMembershipItem groupMembershipItem = new GroupMembershipItem()
            {
                Group = group,
                Member = user,
                GroupUId = group.UId.Value,
                MemberUId = user.UId.Value,
                IsMemberUser = true
            };

            DynamoDbDal dal = new DynamoDbDal
            {
                GroupMembershipTable = _groupMembershipTable
            };

            // Act
            dal.UpsertGroupMembership( groupMembershipItem );
            var retMembership = dal.GetGroupMembership( user.UId.Value, false );
            int count = 0;
            using ( var enumerator = retMembership.GetEnumerator() )
            {
                while ( enumerator.MoveNext() )
                    count++;
            }

            // Assert
            Assert.AreEqual( 0, count );
        }
    }
}
