using System;
using Amazon.DynamoDBv2.Model;
using NUnit.Framework;
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

            User retrievedUser = dal.GetUserByUId( user.UId.Value );
            Assert.AreEqual( retrievedUser.UId, user.UId );
            Assert.AreEqual( retrievedUser.Name, user.Name );
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
    }
}
