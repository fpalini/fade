package fade.kmer.fast

import java.util.Date
import java.util.concurrent.TimeUnit

import fade.util.Value
import fade.util.{SequenceId, _}
import org.apache.spark.util.SizeEstimator

import scala.collection.mutable.{ArrayBuffer, PriorityQueue}
import scala.collection.{JavaConversions, mutable}

object FastKmerUtils {

    def getSuperKmers(k: Int, m: Int, B: Int)(read: Chunk): java.util.List[(BinId, (SequenceId, Data))] = {
        def bin(s: Int) = hash_to_bucket(s, B)

        var out = Array.tabulate[(Int, ArrayBuffer[Kmer])](B)(i => (i, new ArrayBuffer[Kmer]))

        var total:Long = 0

        val norm:Array[Int] = fillNorm(m)


        var lastMmask :Long = (1 << m * 2) - 1
        //keeps upper bound on distinct kmers that could be in a bin (for use with extractSuperKmersHT)

        var min_s:Signature = Signature(-1,-1)
        var super_kmer_start = 0
        var s: Kmer = null
        var N_pos = (-1, -1)
        var i = 0

        val cur: Array[Byte] = read.data

        if (cur.length >= k) {

            min_s = Signature(-1,-1)
            super_kmer_start = 0
            s = null
            N_pos = (-1, -1)
            i = 0

            while (i < cur.length - k + 1) {
                N_pos = firstAndLastOccurrenceOfInvalidNucleotide('N',cur,i,i + k) //DEBUG: not much influence


                if (N_pos._1 != -1) {
                    //there's at least one 'N'

                    if (super_kmer_start < i) {
                        // must output a superkmer



                        out(bin(min_s.value))._2 += new Kmer(i - 1 + k - super_kmer_start,cur,super_kmer_start)

                        //println("[out1] "+longToString(min_s.value) + " - " + new Kmer(i - 1 + k - super_kmer_start,cur,super_kmer_start))


                    }
                    super_kmer_start = i + N_pos._2 + 1 //after last index of N
                    i += N_pos._2 + 1
                }
                else {//we have a valid kmer
                    s = new Kmer(k,cur,i)


                    if (i > min_s.pos) {
                        if (super_kmer_start < i) {
                            out(bin(min_s.value))._2 += new Kmer(i - 1 + k - super_kmer_start,cur,super_kmer_start)
                            //println("[out2] "+longToString(min_s.value) + " - " + new Kmer(i - 1 + k - super_kmer_start,cur,super_kmer_start))

                            super_kmer_start = i


                        }

                        min_s.set(s.getSignature(m,norm),i)

                    }
                    else {

                        val last:Int = s.lastM(lastMmask,norm,m)

                        if (last < min_s.value) {

                            //add superkmer
                            if (super_kmer_start < i) {

                                out(bin(min_s.value))._2 += new Kmer(i - 1 + k - super_kmer_start,cur,super_kmer_start)
                                //println("[out3] "+longToString(min_s.value) + " - " + new Kmer(i - 1 + k - super_kmer_start,cur,super_kmer_start))

                                super_kmer_start = i


                            }

                            min_s.set((last,i + k - m))


                        }
                    }

                    i += 1
                }
            }

            if (cur.length - super_kmer_start >= k) {


                N_pos = firstAndLastOccurrenceOfInvalidNucleotide('N',cur,i,cur.length)

                if(N_pos._1 == -1){
                    out(bin(min_s.value))._2 += new Kmer(cur.length - super_kmer_start, cur, super_kmer_start)
                    //println("[out4] " + longToString(min_s.value) + " - " + new Kmer(cur.length - super_kmer_start, cur, super_kmer_start))

                }
                else if(i + N_pos._1 >= super_kmer_start+k){

                    out(bin(min_s.value))._2 += new Kmer(i + N_pos._1, cur, super_kmer_start)
                    //println("[out5] " + longToString(min_s.value) + " - " + new Kmer(N_pos._1, cur, super_kmer_start))
                }
            }

        }

        //filter empty bins, return iterator
        JavaConversions.bufferAsJavaList(out.filter({case (_,arr) => arr.nonEmpty}).map(t => (new BinId(t._1), (new SequenceId(read.id), new SuperKmers(JavaConversions.bufferAsJavaList(t._2))))).toBuffer)
    }

    def extractKXmersAndCount(k: Int, x: Int, n: Int, canonical: Boolean)(sequencesInBin: (BinId, java.lang.Iterable[(SequenceId, Data)])): java.util.List[(Statistic, (SequenceId, Value))] = {
        // Array that will contain all (k,x)-mers (R)
        val unsortedR: Array[ArrayBuffer[Kmer]] = Array.fill[ArrayBuffer[Kmer]](x + 1)(new ArrayBuffer[Kmer])
        var sortedR = Array.fill[Array[Kmer]](x + 1)(new Array[Kmer](0))

        var sequenceBin: Iterator[FastKmerUtils.Kmer] = null

        var lastOrientation = -1
        var orientation = -1
        var runLength = 0
        var runStart = 0
        var nElementsToSort = 0

        var sk:Kmer = null

        val sequenceCounts = new java.util.ArrayList[(Statistic, (SequenceId, Value))]()

        val sequenceIds = new Array[Int](n);

        for(data <- JavaConversions.iterableAsScalaIterable(sequencesInBin._2)) {
            val sequenceId = data._1.id
            val kmerData = data._2.asInstanceOf[SuperKmers]
            val sequenceArr = kmerData.superkmers

            sequenceBin = JavaConversions.asScalaIterator(sequenceArr.iterator())

            nElementsToSort = 0

            while (sequenceBin.hasNext) {
                //for each super-kmer
                sk = sequenceBin.next

                lastOrientation = -1
                orientation = -1
                runLength = 0

                // the length of a run is 1 if i have a k-mer
                // 2 if i have a k+1 mer
                // 3 if i have a k+2 mer
                // ...

                for (i <- 0 to sk.length - k) {

                    orientation = if (canonical) getOrientation(sk, i, i + k - 1) else 0//getOrientation(skCharArr,i,i+k-1)

                    //check if we need to output
                    if (orientation == lastOrientation) {

                        runLength += 1

                        //if we have reached the biggest subsequence possible, must output it
                        if (runLength == x + 1) {

                            unsortedR(runLength - 1).append(new KmerWithSequence(sequenceId, k + runLength - 1, sk, runStart, runStart + k + runLength - 2, orientation))
                            //println("adding: "+new KmerWithSequence(seqId,k + runLength - 1, sk, runStart, runStart + k + runLength - 2, orientation))
                            nElementsToSort += 1
                            runLength = 0
                            runStart = i
                            lastOrientation = -1
                        }
                    }

                    else {
                        //last orientation was different, must output previous sequence
                        if (lastOrientation != -1) {
                            unsortedR(runLength - 1).append(new KmerWithSequence(sequenceId, k + runLength - 1, sk, runStart, runStart + k + runLength - 2, lastOrientation))
                            //println("adding: "+new KmerWithSequence(seqId,k + runLength - 1, sk, runStart, runStart + k + runLength - 2, lastOrientation))
                            nElementsToSort += 1
                        }

                        //increase run length
                        runLength = 1
                        runStart = i
                        lastOrientation = orientation
                    }

                }
                if (runLength > 0) {
                    unsortedR(runLength - 1).append(new KmerWithSequence(sequenceId, k + runLength - 1, sk, runStart, runStart + k + runLength - 2, lastOrientation))
                    //println("adding: "+new KmerWithSequence(seqId,k + runLength - 1, sk, runStart, runStart + k + runLength - 2, lastOrientation))
                    nElementsToSort += 1
                }
            }

        }//END FOREACH SEQUENCE IN BIN

        for (i <- unsortedR.indices) {
            sortedR(i) = unsortedR(i).toArray
            unsortedR(i).clear()
        }

        for (i <- sortedR.indices){
            scala.util.Sorting.quickSort(sortedR(i))
        }

        val heap = priorityQueueWithIndexes(sortedR, k)

        if (heap.nonEmpty) {
            var index: RIndex = null
            var last_kmer: Kmer = null
            var last_kmer_seq_counts = new Array[Int](sequenceIds.length)

            while (heap.nonEmpty) {
                index = heap.dequeue()

                if (index.pointedKmer == last_kmer) {
                    //increment counter for sequence
                    last_kmer_seq_counts(index.pointedKmer.asInstanceOf[KmerWithSequence].getSequence) += 1

                }
                else {
                    if (last_kmer != null) {

                        //kmer has changed
                        // 1. update sequence similarity with info from last seen kmer
                        for(seqIndex <- last_kmer_seq_counts.indices)
                            sequenceCounts.add((new FastKmer(last_kmer), (new SequenceId(seqIndex), new CountValue(last_kmer_seq_counts(seqIndex)))))

                        //reset count
                        for(seqIndex <- last_kmer_seq_counts.indices) last_kmer_seq_counts(seqIndex)=0

                    }
                    //println("[W] "+last_kmer + " " +last_kmer_cnt)

                    last_kmer = index.pointedKmer
                    last_kmer_seq_counts(last_kmer.asInstanceOf[KmerWithSequence].getSequence) = 1
                }
                index.advance()

                if (!index.exhausted) {
                    //if it still has kmers to read
                    heap.enqueue(index) //put it back in the heap
                }
            }
            //update info for final kmer, and optionally write
            for(seqIndex <- last_kmer_seq_counts.indices)
                sequenceCounts.add((new FastKmer(last_kmer), (new SequenceId(seqIndex), new CountValue(last_kmer_seq_counts(seqIndex)))))
        }


        for (i <- sortedR.indices) {
            sortedR(i) = null
        }

        sequenceCounts
    }

    val nucleotidesPerLong = 31
    val nucleotideBitmasks = new Array[Long](85) //convenience array, 84 is the ASCII code for 'T' (max nucleotide)
    nucleotideBitmasks('A') = 0
    nucleotideBitmasks('C') = 1
    nucleotideBitmasks('G') = 2
    nucleotideBitmasks('T') = 3

    val nucleotideRepr = new Array[Char](4) //char representation of nucleotides
    nucleotideRepr(0) = 'A'
    nucleotideRepr(1) = 'C'
    nucleotideRepr(2) = 'G'
    nucleotideRepr(3) = 'T'

    val nucleotideReprByte = new Array[Byte](4) //byte representation of nucleotides
    nucleotideReprByte(0) = 'A'.toByte
    nucleotideReprByte(1) = 'C'.toByte
    nucleotideReprByte(2) = 'G'.toByte
    nucleotideReprByte(3) = 'T'.toByte


    val nucleotideRC = new Array[Long](4) //reverse complements
      nucleotideRC(0)=3
      nucleotideRC(1)=2
      nucleotideRC(2)=1
      nucleotideRC(3)=0


      val upper2bitMask: Long =  Math.pow(2, 2 * nucleotidesPerLong).toLong - 1

    def is_allowed(_mmer: Int,length:Int) : Boolean = {//TAAGTAC   GTACTTA  AATTCAT
      var mmer = _mmer

      //if((mmer & 0x3f) == 0x3f) // TTT suffix
      //  return false
      //if((mmer & 0x3f) == 0x3b) // TGT suffix
      //  return false
      //if((mmer & 0x3c) == 0x3c) // TT* suffix
      //  return false

      for(j <- 0 until length - 3) {
        if ((mmer & 0xf) == 0) // AA inside
          return false
        else
          mmer >>= 2
      }
        if (mmer == 0) // AAA prefix
          return false
        if (mmer == 0x04) // ACA prefix
          return false

        if ((mmer & 0x3c) == 0) // AA* prefix (NEW)
          return false
        if ((mmer & 0xf) == 0) // *AA prefix
          return false



      true
    }

    def fillNorm(sigLen: Int) :Array[Int] = {
      val norm = new Array[Int](1 << sigLen*2)
      val default_signature = 1 << sigLen * 2

      var rev = 0
      var str_val = 0
      var rev_val = 0
      for(i <- 0 until default_signature) {

        rev = reverse_complement(i, sigLen).toInt

        if (is_allowed(i,sigLen))
          str_val = i
        else str_val = default_signature

        if (is_allowed(rev,sigLen))
          rev_val = rev
        else rev_val = default_signature

        norm(i) = Math.min(str_val, rev_val)

      }
      norm
    }


    def reverse_complement(seq: Long, length: Int): Long =
    {
      var curSeq = seq
      var rev = 0L
      var shift = length * 2 - 2
      for(i <- 0 until length)
      {
        rev += (3 - (curSeq & 3)) << shift
        curSeq >>= 2
        shift -= 2
      }
      rev
    }


    def fillRightOnesMask(runlength:Int):Long={
      var res:Long = 0L
      for(i <- 0 until runlength) {
        res |= (1L << i)
      }
      res
    }




    case class Signature(var value: Int, var pos: Int,offset:Int=0) {
      def set(sigpos: Tuple2[Int, Int],offset:Int = 0): Unit = {
        value = sigpos._1
        pos = sigpos._2 + offset
      }

    }

    //a Scala class representing a Kmer
    class Kmer(val length: Int) extends Serializable with Ordered[Kmer]{

      var _data:Array[Long] = _
      //@transient var origin:Short = -1
      //@transient lazy val padding = if(padded) ((nucleotidesPerLong - (length % nucleotidesPerLong)) % nucleotidesPerLong).toShort else 0

      def _readKmer(length: Int, str: Array[Byte], offset: Int): Array[Long] = {
        val data: Array[Long] = new Array[Long](Math.ceil(length.toDouble / nucleotidesPerLong).toShort)
        //println("Building Kmer with " + Math.ceil(length.toDouble / nucleotidesPerLong).toShort + " longs")

        var currentLng: Long = 0
        var slice = 0

        var i = 0

        var firstSlice: Boolean = true

        while (i < length) {

          currentLng = currentLng << 2

          currentLng |= nucleotideBitmasks(str(offset + i))
          i += 1
          //go to next slice
          if(i % nucleotidesPerLong == 0 || i == length) {
            currentLng &= upper2bitMask //reset upper 2 bits
            data(slice) = currentLng  //save
            currentLng = 0L
            slice += 1
            firstSlice = false
          }

        }
        data
      }

      def readFromKmer(fromKmer:Kmer,startPos:Int,endPos:Int,amt:Int,orientation:Int): Array[Long] ={

        //original kmer is not padded
        val data:Array[Long] = new Array[Long](Math.ceil(amt.toDouble / nucleotidesPerLong).toShort)

        //println"Building Kmer from kmer: " +fromKmer +"("+startPos+","+endPos+") with "+data.length+" longs, length "+amt +" and orientation: "+orientation )

        var (fromEndSlice,fromEndOffset) = fromKmer.getSliceOffset(endPos)
        require((fromEndSlice,fromEndOffset) != (-1,-1),"endPos is invalid")

        var (fromStartSlice,fromStartOffset) = fromKmer.getSliceOffset(startPos)
        require((fromStartSlice,fromStartOffset) != (-1,-1),"startPos is invalid")

        var slice = -1

        val excess = fromKmer.length % nucleotidesPerLong
        val toExcess = amt % nucleotidesPerLong

        val origFinalPadding = if (excess> 0) (nucleotidesPerLong - excess)*2 else 0

        if(orientation == 0){
          //filling from slice = 0
          slice = 0

          if(fromStartOffset == 0){//this is going to be a frequent case
            //start is aligned with start of a long, just copy the whole sequence
            //origin = 1
            while(fromEndSlice-fromStartSlice >= 0){
              data(slice) = fromKmer._data(fromStartSlice)
              fromStartSlice +=1
              slice +=1
            }
            //last long is going to be shifted
            data(data.length-1) = data(data.length-1) >> (2*(nucleotidesPerLong-1) - fromEndOffset)

          }
          else if(fromStartSlice == fromEndSlice){ //falls in one long (frequent for short k)

            data(0) = (fromKmer._data(fromStartSlice)  >> (nucleotidesPerLong*2 - fromEndOffset -2)) & fillRightOnesMask(length*2)

          }
          else{
            //origin = 3
            var curOffset = fromStartOffset
            var curLng:Long = 0L


            while(slice < data.length-1){

              data(slice) = (fromKmer._data(fromStartSlice) << curOffset) & upper2bitMask

              fromStartSlice +=1
              curLng = fromKmer._data(fromStartSlice)

              if(fromStartSlice == fromKmer._data.length-1){// if its last slice from fromKmer, don't shift it too much as it "starts" later
                curLng >>= (nucleotidesPerLong*2 - curOffset - origFinalPadding)
              }
              else curLng = curLng >> nucleotidesPerLong*2 - curOffset

              //fill rightmost zeroed bits
              data(slice) |= curLng

              slice +=1
            }
            //now still last slice is unprocessed (yet all data written is valid
            //we need last nxL-currofbits from fromstartslice, shifted left fromEndOffset to make room
            //take last slice from from, shift right nxl - endpos

            if(fromStartSlice != fromEndSlice){//filling last with last bits from second to last, leaving room for last
              data(slice) = (fromKmer._data(fromStartSlice) & fillRightOnesMask(nucleotidesPerLong*2 - curOffset)) << (fromEndOffset + 2 - (if(fromEndSlice==fromKmer._data.length-1) origFinalPadding else 0))
            }

            //from is last slice, have padding, plus could have some data that has been already put in previous slice (curoffset)
            //need to take into consideration final padding

            data(slice) |= fromKmer._data(fromEndSlice) >> ((nucleotidesPerLong - 1) * 2 - fromEndOffset)
            if (toExcess != 0) {
              data(slice) &= fillRightOnesMask(toExcess*2) //need to enforce last padding + we must remove from last slice what i could have put in previous slice
            }
          }
        }

        else{
          slice = 0
          //reverse orientation
          //origin = 4

          var curLng:Long = fromKmer._data(fromEndSlice) >>(nucleotidesPerLong-1)*2 - fromEndOffset
          val lastNucleotideMask=3L
          var written = 0
          var written_this_slice = 0

          val lastSliceQuantity = if(fromEndSlice != fromKmer._data.length-1) fromEndOffset/2 + 1 else fromEndOffset/2 - origFinalPadding/2 + 1

          while(written < length){
            data(slice) <<= 2
            data(slice) += nucleotideRC((curLng & lastNucleotideMask).toInt)

            written +=1 ; written_this_slice+=1
            curLng >>= 2

            if(written  == lastSliceQuantity || written_this_slice == nucleotidesPerLong){

              fromEndSlice -= 1
              if(fromEndSlice >=0) {
                curLng = fromKmer._data(fromEndSlice)

              }
              written_this_slice = 0
            }

            if(written % nucleotidesPerLong  == 0) {
              slice += 1

            }
          }


        }

        data
      }


      //other constructors
      def this(length: Int,from:Kmer,startPos:Int,endPos:Int,orientation:Int) ={
        this(length)
        _data = readFromKmer(from,startPos,endPos,length,orientation:Int)
      }
      def this(length: Int,str: Array[Byte], offset: Int) = {
        this(length)
        _data = _readKmer(length,str,offset)

      }

      //function to return last m nucleotides from kmer, used to update signatures
      def lastM(mMask: Long,norm: Array[Int],m:Int=0): Int ={

        if(_data.length == 1 || length % nucleotidesPerLong >= m)//optimization: last m will be on right side of last slice
          norm((_data(_data.length - 1) & mMask).toInt)

        else{
          //they are split btw two longs :( need bitwise fill
          var res:Int = 0
          for(i <- length - m until length) {
            res = res << 2
            res |= getNumSymbol(i)

          }

          norm(res)
        }
      }


      def firstM(m:Int):Long = {
        if(_data.length>1 || length == nucleotidesPerLong)
          _data(0) >> (nucleotidesPerLong - m)*2
        else _data(0) >> ((length % nucleotidesPerLong) - m)*2

      }


      def getSignature(sigLen: Int,norm: Array[Int]): (Int,Int) ={
        var mmer = new Mmer(sigLen)(0,norm)
        var pos = 0

        //initialize mmer to first m characters of kmer
        for(i <- 0 until sigLen){
          mmer.insert(getNumSymbol(i))
        }

        var sig = mmer.get()

        for(i <- sigLen until length){
          mmer.insert(getNumSymbol(i))
          if(mmer.get() < sig) {
            sig = mmer.get()
            pos = i - sigLen + 1
          }
          //println(longToString(mmer.get()))
        }
        (sig,pos)
      }


      def getSliceOffset(pos: Int): (Int,Int) ={
        if (pos >= length)
          return  (-1,-1)

        val slice = pos / nucleotidesPerLong
        var offset = -1
        if(slice == _data.length - 1){
          //last long
          offset = (nucleotidesPerLong - (length - pos))*2
        }
        else offset = (pos % nucleotidesPerLong) * 2

        (slice,offset)
      }


      def getNumSymbol(pos: Int): Short={
        val(slice,offset) = getSliceOffset(pos)
        if(slice == -1)
          return -1

        val mask = upper2bitMask >> offset
        var symbol = _data(slice) & mask

        symbol = symbol >> (nucleotidesPerLong *2) - offset -2
        symbol.toShort
      }


       def compare(that: Kmer): Int = {

         def comp(thatData:Array[Long],index:Int): Int = {
           if(index < thatData.length){
             val c:Int = this._data(index) compare thatData(index)
             if(c !=  0) c
             else comp(thatData,index+1)
           }
           else 0
         }


        //println(" Comparing "+this +" and "+that)
        assert(this.length == that.length)
        comp(that._data,0)
      }

      override def equals(that: Any): Boolean =
        that match {
          case that: Kmer => (this compare that) == 0
          case _ => false
        }


      override def hashCode(): Int = _data.toSeq.hashCode()


      def toByteArray: Array[Byte] = {
        val result = new Array[Byte](length)
        val byteMask = 3L
        var binNucleotide = 0L
        var currentLng:Long = 0L
        var slice = _data.length -1

        val excess = length % nucleotidesPerLong

        var i = 0
        var j = length-1
        var amt = if(excess > 0) excess else nucleotidesPerLong

        currentLng = _data(slice)


        while(j >= 0){
          binNucleotide = currentLng & byteMask
          currentLng = currentLng >> 2

          result(j) = nucleotideReprByte(binNucleotide.toInt)

          j-=1
          i+=1

          if(i == amt) {

            slice -= 1
            i=0
            amt = nucleotidesPerLong

            if(slice >= 0) {
              currentLng = _data(slice)
            }
          }
        }

        result
      }

      def toCharArray: Array[Char] = {
        val result = new Array[Char](length)
        val charMask = 3L
        var binNucleotide = 0L
        var currentLng:Long = 0L
        var slice = _data.length -1

        val excess = length % nucleotidesPerLong

        var i = 0
        var j = length-1
        var amt = if(excess > 0) excess else nucleotidesPerLong

        currentLng = _data(slice)


        while(j >= 0){
          binNucleotide = currentLng & charMask
          currentLng = currentLng >> 2

          result(j) = nucleotideRepr(binNucleotide.toInt)

          j-=1
          i+=1

          if(i == amt) {

            slice -= 1
            i=0
            amt = nucleotidesPerLong

            if(slice >= 0) {
              currentLng = _data(slice)
            }
          }
        }

        result
      }

      override def toString: String = {
        //for(i<- _data.indices)
        //    //println(longToString(_data(i)))
        new String(this.toByteArray,0,length) //+ "["+origin+"]"
      }


    }

    //class representing a Kmer with a sequence identifier (for multisequence comparisons)
    class KmerWithSequence(seq: Int,length: Int,from:Kmer,startPos:Int,endPos:Int,orientation:Int) extends Kmer(length,from,startPos,endPos,orientation){
      def getSequence: Int = seq

    }

    class Mmer(val length: Int)(seq: Int, norm: Array[Int]) extends Ordered[Mmer] {


      val mask:Int = (1 << length * 2) - 1

      private var _data:Int = seq
      private var currentVal:Int = norm(seq)

      def get(): Int = currentVal




      override def toString: String = {
        val result = new Array[Char](length)
        val charMask = 3L
        var binNucleotide = 0L
        var currentLng: Long = currentVal
        var j = length -1

        while (j >= 0) {

          binNucleotide = currentLng & charMask
          currentLng = currentLng >> 2

          result(j) = nucleotideRepr(binNucleotide.toInt)

          j -= 1

        }
        result.mkString
      }


      override def compare(that: Mmer): Int = {
        this.get() compare that.get()
      }




      def insert(symb: Short): Unit ={
        _data = _data << 2
        _data += symb
        _data &= mask

        currentVal = norm(_data)
      }

    }

    class RIndex(arr: Array[Kmer],startPos: Int, endPos: Int, shift: Int, kmer_length: Int) extends Ordered[RIndex]{
      /*
      Class that represent a k-mer scanner for the array R
      *
      */

      private var currentPos: Int = startPos
      var pointedKmer: Kmer = _

      _readKmer()


      def advance(): Unit = {
        currentPos +=1
        if(!exhausted)
          _readKmer()
      }

      def _readKmer(): Unit ={
          val nextKmer = arr(currentPos)
          pointedKmer = nextKmer match {
            case nextKmer: KmerWithSequence => new KmerWithSequence(nextKmer.asInstanceOf[KmerWithSequence].getSequence,kmer_length, nextKmer, shift, shift + kmer_length - 1, 0)
            case nextKmer: Kmer => new Kmer(kmer_length, nextKmer, shift, shift + kmer_length - 1, 0)
          }
      }

      def exhausted: Boolean = currentPos >= endPos

      //Indexes are compared lexicographically on the pointed kmer
      def compare(that: RIndex):Int = this.pointedKmer compare that.pointedKmer

      def showOff(): String ={
        printLongBitString(arr(currentPos)._data(0))
        printLongBitString(arr(currentPos)._data(1))
        "[0]  " +longToString(arr(currentPos)._data(0)) + "\n[1]  " +longToString(arr(currentPos)._data(1))+"\n"
      }

      override def toString: String = "[A: " + arr.hashCode() + " start: " + startPos + " end: "+endPos +" shift: "+shift+"] : "+ pointedKmer + "("+currentPos+")"

    }


    object PointedMinOrder extends Ordering[RIndex] {
      /*
      * This object relies on the compare method of RIndex to define a (min) order.
      * It is intended to be used as ordering for the priority of kmer pointers (RIndexes) contained in the heap.
      * The default ordering relies on compare to form a _max_ heap.
      * Therefore the order of items compared must be reversed (y compare x) to enforce a min heap.
      * See the overriden compare method of RIndex for further information.
      * */

      def compare(x:RIndex, y:RIndex):Int = y compare x
    }

    def longToString(num:Long,length:Int=7): String = {
      val result = new Array[Char](nucleotidesPerLong)
      val charMask = 3L
      var binNucleotide = 0L
      var currentLng: Long = num
      var j = nucleotidesPerLong-1

      while (j >= 0) {

        binNucleotide = currentLng & charMask
        currentLng = currentLng >> 2

        result(j) = nucleotideRepr(binNucleotide.toChar)

        j -= 1

      }
      result.mkString
    }
  //convenience class redefine tuple operators
  implicit class TupOps2[A, B](val x: (A, B)) extends AnyVal {
    def :+[C](y: C) = (x._1, x._2, y)
    def +:[C](y: C) = (y, x._1, x._2)
    }


  def priorityQueueWithIndexes(arr: Array[Array[Kmer]],k: Int,binok:Boolean=false): mutable.PriorityQueue[RIndex] = {

    var pos = 0

    var starts = Array[Int]()
    var heap = PriorityQueue.empty[RIndex](PointedMinOrder)

    // for each array
    for (r_index <- arr.indices) {
      val a = arr(r_index)

      if (a.nonEmpty) {
        pos = 0
        starts = Array.fill[Int](r_index)(pos)

        //put one index for prefix (shift = 0) of each array
        heap.enqueue(new RIndex(a, pos, a.length, 0,k))

        if (a.length > 1) {
          for (j <- 1 until a.length) {
            for (i <- 0 until r_index) {
              if (a(j - 1).firstM(i+1) != a(j).firstM(i+1)) {
                val r = new RIndex(a, starts(i), j, i + 1,k)

                heap.enqueue(r)
                starts(i) = j
              }
            }
          }
        }
        for (i <- starts.indices) {
          val r = new RIndex(a, starts(i), a.length, i + 1,k)

          heap.enqueue(r)
        }
      }

    }
    heap
  }


    //UTILS

    def hash_to_bucket(s: Int, B: Int): Int = {
      var key = s
      val c2=0x27d4eb2d; // a prime or an odd constant
      key = (key ^ 61) ^ (key >>> 16)
      key = key + (key << 3)
      key = key ^ (key >>> 4)
      key = key * c2
      key = key ^ (key >>> 15)
      (key & 0x7FFFFFFF) % B
    }

    def notANucleotide(c: Char): Boolean = !(c == 'A' || c == 'C' || c == 'G' || c == 'T')


    def nucleotide_complement(c: Char): Char = c match { case 'A' => 'T' case 'C' => 'G' case 'G' => 'C' case 'T' => 'A' case x => x}

    def getOrientation(s:Array[Byte],i:Int,j:Int): Short = {
      (s(i).toChar, s(j).toChar) match {

        case (start, end) if start < nucleotide_complement(end) => 0
        case (start, end) if start > nucleotide_complement(end) || (i >= j) => 1
        case _ => getOrientation(s, i + 1, j - 1)
      }
    }

    def getOrientation(s:Array[Char],i:Int,j:Int): Short = {
      (s(i), s(j)) match {

        case (start, end) if start < nucleotide_complement(end) => 0
        case (start, end) if start > nucleotide_complement(end) || (i >= j) => 1
        case _ => getOrientation(s, i + 1, j - 1)
      }
    }


  def getOrientation(s:Kmer,i:Int,j:Int): Short = {
    (s.getNumSymbol(i), s.getNumSymbol(j)) match {

      case (start, end) if start < nucleotideRC(end) => 0
      case (start, end) if start > nucleotideRC(end) || (i >= j) => 1
      case _ => getOrientation(s, i + 1, j - 1)
    }
  }

    def orientationsToInt(ors: ArrayBuffer[Short]): Int={
      var res = 0
      for(i <- ors.length -1 to 0 by -1) {
        res = res << 1
        res |= ors(i)
      }
      res
    }

    def firstAndLastOccurrenceOfInvalidNucleotide(c: Char,s:Array[Byte],start:Int,end:Int): (Int,Int) = {
      //finds first and last occurrence of c in s, (-1,-1) if not present
      var (first,last) = (-1,-1)

      for(i <- start until end){
        val c = s(i).toChar
        if (notANucleotide(c))
          if(first == -1) {
            first = i - start
            last = i - start  //the only one seen so far..
          }
          else last = i - start
      }

      (first,last)
    }


    def updateSequenceSimilarity(kmer: Kmer,counts: Array[Int]):Unit = {
      //println("Called updateSequenceSimilarity with kmer: "+kmer +" and counts: "+counts.mkString(" "))
      Unit
    }


  /* DEBUG FUNCTIONS */
  def getDateDiff(date1:Date, date2: Date, timeUnit:TimeUnit): Long = {
    val diffInMillies = date2.getTime() - date1.getTime()
    timeUnit.convert(diffInMillies,TimeUnit.MILLISECONDS)
  }

  def estimateSize(obj: AnyRef): String  ={
    SizeEstimator.estimate(obj) / (1024 * 1024) + "MB"
  }

  //debug
  def printIntBitString(l: Int): Unit ={
    //println(String.format("%032d", new BigInteger(l.toBinaryString)))
  }

  def printLongBitString(l: Long): Unit ={
    //println(String.format("%064d", new BigInteger(l.toBinaryString)))
  }

  def printShortBitString(l: Short): Unit ={
    val i:Int = l
    //println(String.format("%016d", new BigInteger(i.toBinaryString)))
  }

  /* END DEBUG */


  }
