package org.megras.segmentation

import org.apache.batik.parser.AWTPathProducer
import org.megras.util.extensions.equalsEpsilon
import java.awt.Shape
import java.io.StringReader
import java.lang.Double.max
import java.lang.Double.min

sealed class Segmentation(val type: SegmentationType)

data class Rect(val xmin: Double, val xmax: Double, val ymin: Double, val ymax: Double, val zmin: Double = Double.NEGATIVE_INFINITY, val zmax: Double = Double.POSITIVE_INFINITY) : Segmentation(SegmentationType.RECT) {
    constructor(x: Pair<Double, Double>, y: Pair<Double, Double>, z: Pair<Double, Double> = Pair(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY)) : this(x.first, x.second, y.first, y.second, z.first, z.second)
    constructor(min: Triple<Double, Double, Double>, max: Triple<Double, Double, Double>) : this(min.first, max.first, min.second, max.second, min.third, max.third)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Rect

        if (!xmin.equalsEpsilon(other.xmin)) return false
        if (!xmax.equalsEpsilon(other.xmax)) return false
        if (!ymin.equalsEpsilon(other.ymin)) return false
        if (!ymax.equalsEpsilon(other.ymax)) return false
        if (!zmin.equalsEpsilon(other.zmin)) return false
        if (!zmax.equalsEpsilon(other.zmax)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = xmin.hashCode()
        result = 31 * result + xmax.hashCode()
        result = 31 * result + ymin.hashCode()
        result = 31 * result + ymax.hashCode()
        result = 31 * result + zmin.hashCode()
        result = 31 * result + zmax.hashCode()
        return result
    }

    fun to2dPolygon() : Polygon = Polygon(
        listOf(
            xmin to ymin,
            xmax to ymin,
            xmax to ymax,
            xmin to ymax
        )
    )

    val width: Double
        get() = xmax - xmin

    val height: Double
        get() = ymax - ymin


    fun clip(xmin: Double, xmax: Double, ymin: Double, ymax: Double, zmin: Double = Double.NEGATIVE_INFINITY, zmax: Double = Double.POSITIVE_INFINITY): Rect = Rect(
        max(this.xmin, xmin), min(this.xmax, xmax),
        max(this.ymin, ymin), min(this.ymax, ymax),
        max(this.zmin, zmin), min(this.zmax, zmax)
    )

}

data class Polygon(val vertices: List<Pair<Double, Double>>) : Segmentation(SegmentationType.POLYGON) {
    init {
        require(vertices.size > 2) {
            throw IllegalArgumentException ("A polygon needs at least 3 vertices")
        }
    }

    /**
     * Returns 2d bounding [Rect]
     */
    fun boundingRect(): Rect {
        var xmin = vertices.first().first
        var ymin = vertices.first().second
        var xmax = xmin
        var ymax = ymin

        vertices.forEach {
            xmin = min(xmin, it.first)
            xmax = max(xmax, it.first)
            ymin = min(ymin, it.second)
            ymax = max(ymax, it.second)
        }

        return Rect(xmin, xmax, ymin, ymax)

    }

    /**
     * Converts [Polygon] into equivalent 2d [Rect] in case it exists
     */
    fun toRect() : Rect? {

        val verts = this.vertices.toSet()

        if (verts.size != 4) {
            return null
        }

        val bounding = this.boundingRect()

        if (this == bounding.to2dPolygon()) {
            return bounding
        }

        return null

    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Polygon

        if (other.vertices.size != vertices.size) return false

        val start = vertices.indexOfFirst { it.equalsEpsilon(other.vertices.first()) }

        if (start == -1) return false

        return vertices.indices.all { other.vertices[it].equalsEpsilon(vertices[(it + start) % vertices.size]) }

    }

    override fun hashCode(): Int {
        return vertices.sortedBy { it.first }.sortedBy { it.second }.hashCode()
    }

    fun move(dx: Double, dy: Double) : Polygon = Polygon(vertices.map { it.first + dx to it.second + dy })


}

data class Spline(val vertices: List<Pair<Double, Double>>) : Segmentation(SegmentationType.SPLINE) {}

data class Path(val path: Shape) : Segmentation(SegmentationType.PATH) {
    constructor(path: String) : this(AWTPathProducer.createShape(StringReader(path), 0))
}

data class Mask(val mask: ByteArray) : Segmentation(SegmentationType.MASK) {}

data class Channel(val selection: List<String>) : Segmentation(SegmentationType.CHANNEL) {}

data class Time(val intervals: List<Pair<Int, Int>>) : Segmentation(SegmentationType.TIME) {

    fun getTimePointsToSegment() : List<Int> {
        return intervals.flatMap { i -> (i.first until i.second).map { j -> j } }
    }

    fun getTimePointsToDiscard(start: Int, end: Int) : List<Int> {
        val keep = getTimePointsToSegment().sorted()
        var sweeper = 0

        val res = mutableListOf<Int>()
        for (i in start until end) {
            if (sweeper < keep.size && keep[sweeper] == i) {
                sweeper++
            } else {
                res.add(i)
            }
        }
        return res
    }
}

data class Plane(val a: Double, val b: Double, val c: Double, val d: Double, val above: Boolean) : Segmentation(SegmentationType.PLANE) {}