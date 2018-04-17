using System;
using Amazon.DynamoDBv2.Model;
using NUnit.Framework;
using Suplex.Security.Principal;

namespace Suplex.Security.DynamoDbDal.Tests
{
    [TestFixture]
    public class DynamoDbDalTests
    {
        [Test]
        public void UpsertUser_Valid_Succeeds()
        {
            User user = new User()
            {
                Name = "User-",
                IsBuiltIn = true,
                IsEnabled = true,
                IsLocal = true
            };
            user.Name = user.Name + user.UId;

            DynamoDbDal dal = new DynamoDbDal
            {
                TableName = "Suplex.User"
            };
            User upsertedUser = dal.UpsertUser(user);
            Assert.AreEqual(upsertedUser.UId, user.UId);
            Assert.AreEqual(upsertedUser.Name, user.Name);
        }

        [Test]
        public void UpsertUser_NonExistent_Table_Throws_Exception()
        {
            User user = new User()
            {
                Name = "User-",
                IsBuiltIn = true,
                IsEnabled = true,
                IsLocal = true
            };
            user.Name = user.Name + user.UId;

            DynamoDbDal dal = new DynamoDbDal
            {
                TableName = Guid.NewGuid().ToString()
            };

            ResourceNotFoundException ex = Assert.Throws<ResourceNotFoundException>(() => dal.UpsertUser(user));
            StringAssert.Contains( "Requested resource not found: Table", ex.Message );
        }
    }
}
