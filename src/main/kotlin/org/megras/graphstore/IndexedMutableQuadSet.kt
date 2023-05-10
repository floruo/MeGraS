package org.megras.graphstore

import org.megras.data.graph.Quad
import org.megras.data.graph.QuadValue
import org.megras.data.graph.VectorValue
import java.util.concurrent.ConcurrentHashMap
import kotlin.math.min

class IndexedMutableQuadSet : MutableQuadSet {

    private val quads = BasicMutableQuadSet()

    private val sIndex = ConcurrentHashMap<QuadValue, MutableSet<Quad>>()
    private val pIndex = ConcurrentHashMap<QuadValue, MutableSet<Quad>>()
    private val oIndex = ConcurrentHashMap<QuadValue, MutableSet<Quad>>()

    override fun getId(id: Long): Quad? = quads.getId(id)

    override fun filterSubject(subject: QuadValue): QuadSet = BasicMutableQuadSet(sIndex[subject] ?: emptyList())

    override fun filterPredicate(predicate: QuadValue): QuadSet = BasicMutableQuadSet(pIndex[predicate] ?: emptyList())

    override fun filterObject(`object`: QuadValue): QuadSet = BasicMutableQuadSet(oIndex[`object`] ?: emptyList())

    override fun filter(
        subjects: Collection<QuadValue>?,
        predicates: Collection<QuadValue>?,
        objects: Collection<QuadValue>?
    ): QuadSet {

        val sCount = if (subjects?.isEmpty() == true) Int.MAX_VALUE else subjects?.size ?: Int.MAX_VALUE
        val oCount = if (objects?.isEmpty() == true) Int.MAX_VALUE else objects?.size ?: Int.MAX_VALUE


        val sFilter : (Quad) -> Boolean = if (!subjects.isNullOrEmpty()) {{it.subject in subjects }} else {{true}}
        val pFilter : (Quad) -> Boolean = if (!predicates.isNullOrEmpty()) {{it.predicate in predicates }} else {{true}}
        val oFilter : (Quad) -> Boolean = if (!objects.isNullOrEmpty()) {{it.`object` in objects }} else {{true}}


        val minCount = min(sCount, oCount)

        if (minCount == Int.MAX_VALUE) {
            return BasicQuadSet()
        }

        val set = mutableSetOf<Quad>()

        if (subjects.isNullOrEmpty() && objects.isNullOrEmpty()) { //only filtering by predicates
            (predicates ?: emptyList()).flatMapTo(set){
                pIndex[it] ?: emptyList()
            }
            return BasicQuadSet(set)
        }

        return if (sCount < oCount) {
            (subjects ?: emptyList()).flatMapTo(set){ quads ->
                sIndex[quads]?.filter { oFilter(it) && pFilter(it) } ?: emptyList()
            }
            BasicQuadSet(set)
        } else {
            (objects ?: emptyList()).flatMapTo(set){ quads ->
                sIndex[quads]?.filter { sFilter(it) && pFilter(it) } ?: emptyList()
            }
            BasicQuadSet(set)
        }

    }

    override fun toMutable(): MutableQuadSet = this

    override fun toSet(): Set<Quad> = this

    override fun plus(other: QuadSet): QuadSet = BasicMutableQuadSet(this.quads + other)

    override fun nearestNeighbor(predicate: QuadValue, `object`: VectorValue, count: Int, distance: Distance): QuadSet = this.filterPredicate(predicate).nearestNeighbor(predicate, `object`, count, distance)

    override fun textFilter(predicate: QuadValue, objectFilterText: String): QuadSet = filterPredicate(predicate).textFilter(predicate, objectFilterText)

    override val size: Int
        get() = quads.size

    override fun contains(element: Quad): Boolean = quads.contains(element)

    override fun containsAll(elements: Collection<Quad>): Boolean = quads.containsAll(elements)

    override fun isEmpty(): Boolean = quads.isEmpty()

    override fun iterator(): MutableIterator<Quad> = quads.iterator()

    override fun add(element: Quad): Boolean {

        if(quads.add(element)) {
            if (sIndex.containsKey(element.subject)) {
                sIndex[element.subject]!!.add(element)
            } else {
                sIndex[element.subject] = mutableSetOf(element)
            }

            if (pIndex.containsKey(element.predicate)) {
                pIndex[element.predicate]!!.add(element)
            } else {
                pIndex[element.predicate] = mutableSetOf(element)
            }

            if (oIndex.containsKey(element.`object`)) {
                oIndex[element.`object`]!!.add(element)
            } else {
                oIndex[element.`object`] = mutableSetOf(element)
            }
            return true
        }
        return false

    }

    override fun addAll(elements: Collection<Quad>): Boolean {
        var changed = false
        for(q in elements) {
            changed = this.add(q) || changed
        }
        return changed
    }

    override fun clear() {
        quads.clear()
        sIndex.clear()
        pIndex.clear()
        oIndex.clear()
    }

    override fun remove(element: Quad): Boolean {
        TODO("Not yet implemented")
    }

    override fun removeAll(elements: Collection<Quad>): Boolean {
        TODO("Not yet implemented")
    }

    override fun retainAll(elements: Collection<Quad>): Boolean {
        TODO("Not yet implemented")
    }
}