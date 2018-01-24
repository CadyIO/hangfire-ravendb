using Raven.Client;
using Raven.Client.Documents.Session;
using System;
using System.Collections.Generic;
using System.Text;

namespace Hangfire.Raven.Extensions {
    public static class IDocumentSessionExtensions {
        private static IMetadataDictionary GetMetadata<T>(this IDocumentSession session, string id) => session.Advanced.GetMetadataFor(session.Load<T>(id));

        private static IMetadataDictionary GetMetadata<T>(this IDocumentSession session, T obj) => session.Advanced.GetMetadataFor(obj);

        public static void SetExpiry<T>(this IDocumentSession session, string id, TimeSpan expireIn) {
            var metadata = session.GetMetadata<T>(id);
            metadata[Constants.Documents.Metadata.Expires] = (DateTime.UtcNow + expireIn).ToString("O");
        }

        public static void SetExpiry<T>(this IDocumentSession session, T obj, TimeSpan expireIn) {
            var metadata = session.GetMetadata(obj);
            metadata[Constants.Documents.Metadata.Expires] = (DateTime.UtcNow + expireIn).ToString("O");
        }

        public static void RemoveExpiry<T>(this IDocumentSession session, string id) {
            var metadata = session.GetMetadata<T>(id);
            metadata.Remove(Constants.Documents.Metadata.Expires);
        }

        public static DateTime GetExpiry<T>(this IDocumentSession session, string id) {
            var metadata = session.GetMetadata<T>(id);
            if (metadata.ContainsKey(id))
                return (DateTime)metadata[Constants.Documents.Metadata.Expires];
            else
                return default(DateTime);
        }

        public static DateTime GetExpiry<T>(this IDocumentSession session, T obj) {
            var metadata = GetMetadata(obj);
            if (metadata.ContainsKey(Constants.Documents.Metadata.Expires))
                return (DateTime)metadata[Constants.Documents.Metadata.Expires];
            else
                return default(DateTime);
        }
    }
}
