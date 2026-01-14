<script setup lang="ts">
import { ref } from 'vue'
import { useRouter } from 'vue-router'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle, CardFooter } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { backendUrl } from '@/store/music'

const router = useRouter()

const selectedFile = ref<File | null>(null)
const isUploading = ref(false)
const errorMessage = ref('')

const handleFileChange = (event: Event) => {
  const target = event.target as HTMLInputElement
  if (target.files && target.files.length > 0) {
    selectedFile.value = target.files[0]
    errorMessage.value = ''
  }
}

const handleAddMusic = async () => {
  if (!selectedFile.value) {
    errorMessage.value = 'Please select a file'
    return
  }

  isUploading.value = true
  errorMessage.value = ''

  try {
    const formData = new FormData()
    formData.append('file', selectedFile.value)

    const response = await fetch(`${backendUrl.value}/add-song?wait=false`, {
      method: 'POST',
      headers: {
        Accept: 'application/json'
      },
      body: formData
    })

    if (response.ok) {
      const data = await response.json()
      console.log('Song added:', data)
      selectedFile.value = null
      router.push('/compare')
    } else {
      errorMessage.value = `Error: ${response.status} ${response.statusText}`
    }
  } catch (err: any) {
    errorMessage.value = `Upload failed: ${err.message}`
  } finally {
    isUploading.value = false
  }
}
</script>

<template>
  <div class="max-w-2xl mx-auto">
    <Card class="bg-white shadow-2xl">
      <CardHeader>
        <CardTitle class="text-3xl text-blue-900">Add New Track</CardTitle>
        <CardDescription class="text-gray-600">
          Upload an audio file to discover similar tracks
        </CardDescription>
      </CardHeader>
      <CardContent class="space-y-6">
        <div class="space-y-2">
          <label class="text-sm font-semibold text-gray-700">Audio File</label>
          <Input 
            type="file"
            @change="handleFileChange"
            accept="audio/*"
            class="h-12 text-lg cursor-pointer"
          />
          <p v-if="selectedFile" class="text-sm text-blue-600">
            Selected: {{ selectedFile.name }}
          </p>
        </div>
        <div v-if="errorMessage" class="p-4 bg-red-50 border border-red-200 rounded-lg">
          <p class="text-red-600">{{ errorMessage }}</p>
        </div>
      </CardContent>
      <CardFooter>
        <Button 
          @click="handleAddMusic" 
          :disabled="!selectedFile || isUploading"
          class="w-full h-14 text-lg bg-gradient-to-r from-blue-600 to-blue-700 hover:from-blue-700 hover:to-blue-800 text-white disabled:opacity-50"
        >
          <span class="mr-2">{{ isUploading ? '⏳' : '+' }}</span> 
          {{ isUploading ? 'Uploading...' : 'Add to Library' }}
        </Button>
      </CardFooter>
    </Card>
  </div>
</template>
