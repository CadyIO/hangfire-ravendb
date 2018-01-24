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
            SetExpiry(session.GetMetadata(id), expireIn);
        }

        public static void SetExpiry<T>(this IDocumentSession session, T obj, TimeSpan expireIn) {
            SetExpiry(session.GetMetadata(obj), expireIn);
        }

        private static void SetExpiry(IMetadataDictionary metadata, TimeSpan expireIn) {
            metadata[Constants.Documents.Metadata.Expires] = (DateTime.UtcNow + expireIn).ToString("O");
        }

        public static void RemoveExpiry<T>(this IDocumentSession session, string id) {
            var metadata = session.GetMetadata<T>(id);
            metadata.Remove(Constants.Documents.Metadata.Expires);
        }

        public static DateTime? GetExpiry<T>(this IDocumentSession session, string id) {
            return session.GetExpiry(session.GetMetadata<T>(id));
        }

        public static DateTime? GetExpiry<T>(this IDocumentSession session, T obj) {
            return session.GetExpiry(session.GetMetadata(obj));
        }

        private static DateTime? GetExpiry(IMetadataDictionary metadata) {
            if (metadata.ContainsKey(Constants.Documents.Metadata.Expires))
                return DateTime.Parse(metadata[Constants.Documents.Metadata.Expires].ToString());
            else
                return null;
        }
    }
}
