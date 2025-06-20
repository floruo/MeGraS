package org.megras.data.graph

import java.io.Serializable
import org.megras.data.model.Config

open class LocalQuadValue(public override val uri: String, infix: String = "") : URIValue(LocalQuadValue.defaultPrefix, if (infix.isNotEmpty()) "${infix.trim()}/${clean(uri)}" else clean(uri)), Serializable{

    companion object {

        private fun clean(uri: String) : String {
            val clean = uri.trim()
            return if (clean.startsWith('/')) {
                clean.substring(1)
            } else {
                clean
            }
        }

        private val config: Config
            get() = Config()

        val defaultPrefix: String
            get() = "http://${config.hostName}:${config.httpPort}/"
    }


    override fun prefix() = defaultPrefix
    override fun suffix() = uri

    override fun toString() = "<${defaultPrefix}${uri}>"

    fun toPath() = "${defaultPrefix}${uri}"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        if (!super.equals(other)) return false

        other as LocalQuadValue

        return uri == other.uri
    }

    override fun hashCode(): Int {
        var result = super.hashCode()
        result = 31 * result + uri.hashCode()
        return result
    }

}